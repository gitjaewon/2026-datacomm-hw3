import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Worker {
    static final int CONNECTING = 0, RING_SETUP = 1, RUNNING = 2, CRASHED = 3, REJOINING = 4;
    static final String[] STATE = {"CONNECTING", "RING_SETUP", "RUNNING", "CRASHED", "REJOINING"};

    static class Item {
        final int k, e, startStep, partial;

        Item(int k, int e, int startStep, int partial) {
            this.k = k;
            this.e = e;
            this.startStep = startStep;
            this.partial = partial;
        }
    }

    static class Ckpt {
        final int e, s, partial;

        Ckpt(int e, int s, int partial) {
            this.e = e;
            this.s = s;
            this.partial = partial;
        }
    }

    // 소켓 하나에 쓰는 쪽. 여러 Thread가 같이 쓰므로 Lock
    static class Out {
        final Socket sock;
        final BufferedWriter w;
        final ReentrantLock lock = new ReentrantLock();

        Out(Socket sock) throws IOException {
            this.sock = sock;
            w = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8));
        }

        void send(String msg) throws IOException {
            lock.lock();
            try {
                w.write(msg);
                w.write('\n');
                w.flush();
            } finally {
                lock.unlock();
            }
        }
    }

    final int id;
    final String coordHost;
    final int coordPort;
    final String peerIp;
    final int peerPort;
    final int stepDelay;
    final Log log;

    // Lock 순서: qLock -> stateLock -> storeLock
    final ReentrantLock stateLock = new ReentrantLock();
    final Condition stateCond = stateLock.newCondition();
    int state = CONNECTING;
    int crashGen = 0;
    // 송신 중에는 공유 상태 Lock을 놓는다. CRASH는 승인된 송신이 끝난 뒤 확정한다.
    boolean crashPending = false;
    int activeSends = 0;
    boolean stopping = false;
    boolean startReceived = false, launcherConnected = false, readySent = false;

    final ReentrantLock qLock = new ReentrantLock();
    final Condition qCond = qLock.newCondition();
    final ArrayDeque<Item> localQ = new ArrayDeque<>();
    boolean requested = false;
    boolean noTask = false;
    final Set<Integer> waitingAck = new HashSet<>();

    final ReentrantLock storeLock = new ReentrantLock();
    final Map<Integer, Ckpt> store = new HashMap<>();

    Out coord, buddy, launcher;
    int buddyId;
    ServerSocket peerServer;

    // 현재 계산 중인 Task (CRASH 로그용)
    volatile String current = "none";

    // 통계 (stateLock으로 보호)
    int ckptSent = 0, ckptRecv = 0, ckptDropped = 0, ackCount = 0, rejectCount = 0, crashCount = 0;
    int takeoverCount = 0, resultCount = 0;
    int ckptSentWindow = 0, ckptRecvWindow = 0;

    Worker(int id, String coordHost, int coordPort, String peerIp, int peerPort, int stepDelay, String logPath) throws IOException {
        this.id = id;
        this.coordHost = coordHost;
        this.coordPort = coordPort;
        this.peerIp = peerIp;
        this.peerPort = peerPort;
        this.stepDelay = stepDelay;
        log = new Log("WORKER" + id, logPath, false);
    }

    static Socket connectRetry(String host, int port) throws InterruptedException {
        while (true) {
            try {
                Socket s = new Socket();
                s.connect(new InetSocketAddress(host, port), 3000);
                s.setTcpNoDelay(true);
                return s;
            } catch (IOException e) {
                Thread.sleep(500);
            }
        }
    }

    void run() throws Exception {
        log.info("INIT", String.format("worker %d start. coord=%s:%d peer=%s:%d step_delay=%dms small_primes=%d",
                id, coordHost, coordPort, peerIp, peerPort, stepDelay, Primes.SMALL.length));
        // Peer 포트를 먼저 열고 Coordinator에 접속
        peerServer = new ServerSocket();
        peerServer.bind(new InetSocketAddress(InetAddress.getByName(peerIp), peerPort));
        Thread ps = new Thread(this::peerServerLoop, "PeerServer");
        ps.setDaemon(true);
        ps.start();

        Socket cs = connectRetry(coordHost, coordPort);
        coord = new Out(cs);
        coord.send("HELLO " + id + " " + peerIp + " " + peerPort);
        log.ok("CONNECT", "connected to coordinator. HELLO " + id + " " + peerIp + " " + peerPort);

        Thread hb = new Thread(this::heartbeatLoop, "Heartbeat");
        Thread comp = new Thread(this::computeLoop, "Compute");
        hb.setDaemon(true);
        comp.setDaemon(true);
        hb.start();
        comp.start();

        controlRecvLoop(new BufferedReader(new InputStreamReader(cs.getInputStream(), StandardCharsets.UTF_8)));
    }

    void sendCoord(String msg) {
        try {
            coord.send(msg);
        } catch (IOException e) {
            // Coordinator 연결 오류는 치명적
            log.fail("CONNECT", "coordinator send failed: " + e.getMessage() + ". exit");
            System.exit(1);
        }
    }

    void sendLauncher(String msg) {
        Out l;
        stateLock.lock();
        try {
            l = launcher;
        } finally {
            stateLock.unlock();
        }
        if (l == null) return;
        try {
            l.send(msg);
        } catch (IOException e) {
            log.warn("CONNECT", "launcher send failed: " + e.getMessage());
        }
    }

    // Socket 송신 차례를 얻은 뒤 승인한다. CRASH 대기 중에는 새 송신을 승인하지 않는다.
    boolean sendIfAlive(int gen, Out target, String msg) {
        target.lock.lock();
        try {
            stateLock.lock();
            try {
                if (state != RUNNING || crashPending || stopping || crashGen != gen) return false;
                activeSends++;
            } finally {
                stateLock.unlock();
            }
            try {
                target.send(msg);
            } catch (IOException e) {
                if (target == coord) {
                    log.fail("CONNECT", "coordinator send failed: " + e.getMessage() + ". exit");
                    System.exit(1);
                } else {
                    log.warn("CKPT", "send to buddy W" + buddyId + " failed: " + e.getMessage());
                }
            } finally {
                stateLock.lock();
                try {
                    activeSends--;
                    stateCond.signalAll();
                } finally {
                    stateLock.unlock();
                }
            }
            return true;
        } finally {
            target.lock.unlock();
        }
    }

    void checkReady() {
        boolean send = false;
        stateLock.lock();
        try {
            if (startReceived && launcherConnected && !readySent) {
                readySent = true;
                send = true;
            }
        } finally {
            stateLock.unlock();
        }
        if (send) {
            sendLauncher("READY " + id);
            log.info("START", "READY " + id + " sent to launcher");
        }
    }

    int getState() {
        stateLock.lock();
        try {
            return state;
        } finally {
            stateLock.unlock();
        }
    }

    // Coordinator 메시지 처리
    void controlRecvLoop(BufferedReader in) {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] t = line.split(" ");
                if (t[0].equals("BYE")) {
                    onBye();
                    return;
                }
                if (getState() == CRASHED) continue; // crash 중에는 읽고 버림
                try {
                    switch (t[0]) {
                        case "RING" -> onRing(Integer.parseInt(t[1]), t[2], Integer.parseInt(t[3]));
                        case "START" -> onStart();
                        case "ASSIGN" -> onAssign(Integer.parseInt(t[1]), Integer.parseInt(t[2]));
                        case "TAKEOVER" -> onTakeover(Integer.parseInt(t[1]), Integer.parseInt(t[2]), Integer.parseInt(t[3].substring(5)));
                        case "ACK" -> onAck(Integer.parseInt(t[1]));
                        case "REJECT_STALE" -> onReject(Integer.parseInt(t[1]), t[2]);
                        case "NO_TASK" -> onNoTask();
                        case "WELCOME" -> onWelcome();
                        case "RETRY_LATER" -> onRetryLater();
                        default -> log.warn("CONNECT", "unknown message: " + line);
                    }
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    log.warn("CONNECT", "bad message: " + line);
                }
            }
            log.fail("CONNECT", "coordinator closed connection. exit");
        } catch (IOException e) {
            log.fail("CONNECT", "coordinator connection error: " + e.getMessage() + ". exit");
        } catch (InterruptedException e) {
            return;
        }
        System.exit(1);
    }

    void onRing(int j, String ip, int port) throws InterruptedException, IOException {
        stateLock.lock();
        try {
            state = RING_SETUP;
        } finally {
            stateLock.unlock();
        }
        buddyId = j;
        Socket s = connectRetry(ip, port);
        buddy = new Out(s);
        buddy.send("PEER " + id);
        sendCoord("RING_OK " + id);
        log.ok("RING", "buddy W" + j + " (" + ip + ":" + port + ") connected. RING_OK " + id);
    }

    void onStart() {
        stateLock.lock();
        try {
            state = RUNNING;
            startReceived = true;
            stateCond.signalAll();
        } finally {
            stateLock.unlock();
        }
        qLock.lock();
        try {
            qCond.signalAll();
        } finally {
            qLock.unlock();
        }
        log.ok("START", "START received. state=RUNNING");
        checkReady();
    }

    void onAssign(int k, int e) {
        qLock.lock();
        try {
            localQ.add(new Item(k, e, 1, 0));
            requested = false; // 안 내리면 다음 REQ를 못 보냄
            qCond.signalAll();
        } finally {
            qLock.unlock();
        }
        log.ok("ASSIGN", "task=" + k + " e" + e + " received. range=[" + Primes.stepLo(k, 1) + ", " + (Primes.stepLo(k, 10) + 9999) + "]");
    }

    void onTakeover(int k, int e, int from) {
        Ckpt ck;
        qLock.lock();
        try {
            storeLock.lock();
            try {
                ck = store.get(k);
            } finally {
                storeLock.unlock();
            }
            if (ck != null) localQ.add(new Item(k, e, ck.s + 1, ck.partial));
            else localQ.add(new Item(k, e, 1, 0));
            qCond.signalAll();
        } finally {
            qLock.unlock();
        }
        stateLock.lock();
        try {
            takeoverCount++;
        } finally {
            stateLock.unlock();
        }
        if (ck != null)
            log.ok("TAKEOVER", String.format("task=%d e%d from W%d. ckpt=(e%d, step %d, %d) -> resume step %d",
                    k, e, from, ck.e, ck.s, ck.partial, ck.s + 1));
        else
            log.ok("TAKEOVER", String.format("task=%d e%d from W%d. no ckpt -> start step 1", k, e, from));
    }

    void onAck(int k) {
        boolean known;
        qLock.lock();
        try {
            known = waitingAck.remove(k);
        } finally {
            qLock.unlock();
        }
        if (!known) {
            log.info("COMMIT", "ACK " + k + " for unknown task. ignored");
            return;
        }
        stateLock.lock();
        try {
            ackCount++;
        } finally {
            stateLock.unlock();
        }
        log.ok("COMMIT", "ACK " + k + " (committed)");
    }

    void onReject(int k, String reason) {
        boolean known;
        qLock.lock();
        try {
            known = waitingAck.remove(k);
        } finally {
            qLock.unlock();
        }
        if (!known) {
            log.info("REJECT_STALE", "REJECT_STALE " + k + " " + reason + " for unknown task. ignored");
            return;
        }
        stateLock.lock();
        try {
            rejectCount++;
        } finally {
            stateLock.unlock();
        }
        log.warn("REJECT_STALE", "task=" + k + " rejected. reason=" + reason);
    }

    void onNoTask() {
        qLock.lock();
        try {
            noTask = true;
            requested = false;
        } finally {
            qLock.unlock();
        }
        log.info("ASSIGN", "NO_TASK. no more REQ, waiting for TAKEOVER or BYE");
    }

    void onWelcome() {
        stateLock.lock();
        try {
            if (state != REJOINING) return;
            state = RUNNING;
            stateCond.signalAll();
        } finally {
            stateLock.unlock();
        }
        log.ok("WELCOME", "rejoin accepted. state=RUNNING, HB resumed. REJOINED " + id + " -> launcher");
        sendLauncher("REJOINED " + id);
        qLock.lock();
        try {
            qCond.signalAll();
        } finally {
            qLock.unlock();
        }
    }

    void onRetryLater() {
        log.warn("REJOIN", "RETRY_LATER. resend REJOIN after 2s");
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                return;
            }
            if (getState() == REJOINING) {
                sendCoord("REJOIN " + id);
                log.info("REJOIN", "REJOIN " + id + " resent");
            }
        });
        t.setDaemon(true);
        t.start();
    }

    void onBye() {
        stateLock.lock();
        try {
            stopping = true;
            stateCond.signalAll();
        } finally {
            stateLock.unlock();
        }
        qLock.lock();
        try {
            qCond.signalAll();
        } finally {
            qLock.unlock();
        }
        String summary;
        stateLock.lock();
        try {
            summary = String.format("BYE received (state=%s). results=%d ack=%d reject=%d ckpt_sent=%d ckpt_recv=%d ckpt_dropped=%d takeover=%d crashes=%d",
                    STATE[state], resultCount, ackCount, rejectCount, ckptSent, ckptRecv, ckptDropped, takeoverCount, crashCount);
        } finally {
            stateLock.unlock();
        }
        log.ok("TERMINATE", summary);
        log.close();
        closeQuietly(coord);
        closeQuietly(buddy);
        closeQuietly(launcher);
        try {
            peerServer.close();
        } catch (IOException ignored) {
        }
        System.exit(0);
    }

    static void closeQuietly(Out o) {
        if (o == null) return;
        try {
            o.sock.close();
        } catch (IOException ignored) {
        }
    }

    // Peer 포트: 선행 Worker의 CKPT 연결, Launcher 제어 연결
    void peerServerLoop() {
        while (true) {
            try {
                Socket s = peerServer.accept();
                s.setTcpNoDelay(true);
                Thread t = new Thread(() -> peerConn(s), "Peer-" + s.getPort());
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    void peerConn(Socket s) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            String first = in.readLine();
            if (first == null) return;
            first = first.trim();
            if (first.startsWith("PEER")) {
                log.ok("RING", "predecessor W" + first.substring(5) + " connected (CKPT link)");
                String line;
                while ((line = in.readLine()) != null) {
                    String[] t = line.trim().split(" ");
                    if (!t[0].equals("CKPT")) continue;
                    try {
                        onCkpt(Integer.parseInt(t[1]), Integer.parseInt(t[2]), Integer.parseInt(t[3]), Integer.parseInt(t[4]));
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        log.warn("CKPT", "bad CKPT: " + line);
                    }
                }
            } else if (first.equals("LAUNCHER")) {
                stateLock.lock();
                try {
                    launcher = new Out(s);
                    launcherConnected = true;
                } finally {
                    stateLock.unlock();
                }
                log.ok("CONNECT", "launcher control connection");
                checkReady();
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    if (line.equals("CRASH")) onCrash();
                    else if (line.equals("RESTART")) onRestart();
                }
            }
        } catch (IOException e) {
            // 연결 끊김. Launcher 종료 등
        }
    }

    void onCkpt(int k, int e, int s, int partial) {
        stateLock.lock();
        try {
            if (state == CRASHED) {
                ckptDropped++;
                return;
            }
            ckptRecv++;
            ckptRecvWindow++;
        } finally {
            stateLock.unlock();
        }
        storeLock.lock();
        try {
            Ckpt old = store.get(k);
            if (old == null || e > old.e || (e == old.e && s > old.s)) store.put(k, new Ckpt(e, s, partial));
        } finally {
            storeLock.unlock();
        }
    }

    void onCrash() {
        int qn, sn;
        stateLock.lock();
        try {
            if (state == CRASHED || crashPending) return;
            crashPending = true;
            // await는 stateLock을 놓으므로 송신 완료와 HB/Compute 상태 확인을 막지 않는다.
            while (activeSends > 0) stateCond.awaitUninterruptibly();
            state = CRASHED;
            crashGen++;
            crashCount++;
            crashPending = false;
        } finally {
            stateLock.unlock();
        }
        qLock.lock();
        try {
            qn = localQ.size();
            localQ.clear();
            requested = false;
            noTask = false;
            waitingAck.clear();
        } finally {
            qLock.unlock();
        }
        storeLock.lock();
        try {
            sn = store.size();
            store.clear();
        } finally {
            storeLock.unlock();
        }
        log.warn("CRASH", "CRASH from launcher. cleared current=" + current + ", queue=" + qn + ", store=" + sn + ". silent (connections kept)");
    }

    void onRestart() {
        stateLock.lock();
        try {
            if (state != CRASHED) return;
            state = REJOINING;
        } finally {
            stateLock.unlock();
        }
        log.info("RESTART", "RESTART from launcher. state=REJOINING, queue/store empty");
        sendCoord("REJOIN " + id);
        log.info("REJOIN", "REJOIN " + id + " sent");
    }

    // RUNNING일 때만 1초마다 HB. 10초마다 CKPT 건수 요약
    void heartbeatLoop() {
        int tick = 0;
        while (true) {
            int gen;
            stateLock.lock();
            try {
                while (state != RUNNING && !stopping) stateCond.await();
                if (stopping) return;
                gen = crashGen;
            } catch (InterruptedException e) {
                return;
            } finally {
                stateLock.unlock();
            }
            sendIfAlive(gen, coord, "HB " + id);
            tick++;
            if (tick % 10 == 0) {
                int sent, recv;
                stateLock.lock();
                try {
                    sent = ckptSentWindow;
                    recv = ckptRecvWindow;
                    ckptSentWindow = ckptRecvWindow = 0;
                } finally {
                    stateLock.unlock();
                }
                log.info("CKPT", "last 10s: sent " + sent + " to W" + buddyId + ", received " + recv);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    // Task를 꺼내는 순간의 crashGen을 같이 돌려준다
    Object[] takeTask() throws InterruptedException {
        while (true) {
            stateLock.lock();
            try {
                while (state != RUNNING && !stopping) stateCond.await();
            } finally {
                stateLock.unlock();
            }
            boolean needReq;
            int gen;
            qLock.lock();
            try {
                boolean running;
                stateLock.lock();
                try {
                    if (stopping) return null;
                    running = state == RUNNING;
                    gen = crashGen;
                } finally {
                    stateLock.unlock();
                }
                if (running && !localQ.isEmpty()) return new Object[]{localQ.poll(), gen};
                needReq = running && localQ.isEmpty() && !requested && !noTask;
                if (needReq) requested = true;
                else qCond.await();
            } finally {
                qLock.unlock();
            }
            if (needReq && sendIfAlive(gen, coord, "REQ " + id)) log.info("ASSIGN", "REQ " + id + " sent (local queue empty)");
        }
    }

    void computeLoop() {
        try {
            while (true) {
                Object[] got = takeTask();
                if (got == null) return;
                Item it = (Item) got[0];
                int gen = (Integer) got[1];
                int partial = it.partial;
                boolean finished = true;
                for (int s = it.startStep; s <= 10; s++) {
                    if (crashGenNow() != gen) {
                        finished = false;
                        break;
                    }
                    current = "task " + it.k + " e" + it.e + " step " + s;
                    partial += Primes.count(it.k, s);
                    Thread.sleep(stepDelay);
                    if (s < 10) {
                        if (!sendIfAlive(gen, buddy, "CKPT " + it.k + " " + it.e + " " + s + " " + partial)) {
                            finished = false;
                            break;
                        }
                        countCkptSent();
                    }
                }
                current = "none";
                if (!finished) continue;
                qLock.lock();
                try {
                    waitingAck.add(it.k);
                } finally {
                    qLock.unlock();
                }
                // ACK가 먼저 찍히지 않게 로그를 먼저 남긴다
                log.ok("RESULT", "task=" + it.k + " e" + it.e + " value=" + partial
                        + (it.startStep > 1 ? " (resumed from step " + it.startStep + ")" : ""));
                if (sendIfAlive(gen, coord, "RESULT " + it.k + " " + it.e + " " + partial)) {
                    stateLock.lock();
                    try {
                        resultCount++;
                    } finally {
                        stateLock.unlock();
                    }
                } else {
                    log.warn("RESULT", "task=" + it.k + " not sent (crashed)");
                    qLock.lock();
                    try {
                        waitingAck.remove(it.k);
                    } finally {
                        qLock.unlock();
                    }
                }
            }
        } catch (InterruptedException ignored) {
        }
    }

    int crashGenNow() {
        stateLock.lock();
        try {
            return crashGen;
        } finally {
            stateLock.unlock();
        }
    }

    void countCkptSent() {
        stateLock.lock();
        try {
            ckptSent++;
            ckptSentWindow++;
        } finally {
            stateLock.unlock();
        }
    }

    public static void main(String[] args) throws Exception {
        int id = 0;
        String coordAddr = "127.0.0.1:8000";
        String peerIp = "127.0.0.1";
        int peerPort = -1;
        int stepDelay = 100;
        String logDir = "logs";
        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "--id" -> id = Integer.parseInt(args[++a]);
                case "--coord" -> coordAddr = args[++a];
                case "--peer-ip" -> peerIp = args[++a];
                case "--peer-port" -> peerPort = Integer.parseInt(args[++a]);
                case "--step-delay" -> stepDelay = Integer.parseInt(args[++a]);
                case "--log-dir" -> logDir = args[++a];
                default -> {
                    System.out.println("usage: java Worker --id i --coord host:port [--peer-ip ip] [--peer-port 7000+i] [--step-delay 100] [--log-dir logs]");
                    return;
                }
            }
        }
        if (id < 1 || id > 10) {
            System.out.println("--id must be 1..10");
            return;
        }
        if (peerPort < 0) peerPort = 7000 + id;
        int c = coordAddr.lastIndexOf(':');
        new Worker(id, coordAddr.substring(0, c), Integer.parseInt(coordAddr.substring(c + 1)),
                peerIp, peerPort, stepDelay, logDir + "/Worker" + id + ".txt").run();
    }
}
