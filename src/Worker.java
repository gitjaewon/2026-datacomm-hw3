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

    static class Out {
        final Socket sock;
        final BufferedWriter w;
        final ReentrantLock lock = new ReentrantLock();

        Out(Socket sock) throws IOException {
            this.sock = sock;
            w = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8));
        }

        // 한 줄 전송
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

    final ReentrantLock stateLock = new ReentrantLock();
    final Condition stateCond = stateLock.newCondition();
    int state = CONNECTING;
    int crashGen = 0;
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

    volatile String current = "none";

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

    // 접속될 때까지 0.5초마다 재시도
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

    // Peer 포트 열고 Coordinator 접속, Thread 시작
    void run() throws Exception {
        log.info("INIT", String.format("worker %d start. coord=%s:%d peer=%s:%d step_delay=%dms small_primes=%d",
                id, coordHost, coordPort, peerIp, peerPort, stepDelay, Primes.SMALL.length));
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

    // Coordinator에게 전송 (실패하면 종료)
    void sendCoord(String msg) {
        try {
            coord.send(msg);
        } catch (IOException e) {
            log.fail("CONNECT", "coordinator send failed: " + e.getMessage() + ". exit");
            System.exit(1);
        }
    }

    // Launcher에게 전송
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

    // RUNNING이고 crash 세대가 같을 때만 전송
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

    // START와 Launcher 연결이 모두 되면 READY 전송
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

    // 현재 상태
    int getState() {
        stateLock.lock();
        try {
            return state;
        } finally {
            stateLock.unlock();
        }
    }

    // 메시지를 받은 시점의 crash 세대 (crash 중이면 -1)
    int messageGeneration() {
        stateLock.lock();
        try {
            return state == CRASHED ? -1 : crashGen;
        } finally {
            stateLock.unlock();
        }
    }

    // 받은 뒤 crash/rejoin이 없었는지 확인
    boolean acceptsMessage(int gen) {
        return gen >= 0 && state != CRASHED && crashGen == gen;
    }

    // Coordinator 메시지 수신
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
                int gen = messageGeneration();
                if (gen < 0) continue;
                try {
                    switch (t[0]) {
                        case "RING" -> onRing(Integer.parseInt(t[1]), t[2], Integer.parseInt(t[3]));
                        case "START" -> onStart();
                        case "ASSIGN" -> onAssign(Integer.parseInt(t[1]), Integer.parseInt(t[2]), gen);
                        case "TAKEOVER" -> onTakeover(Integer.parseInt(t[1]), Integer.parseInt(t[2]), Integer.parseInt(t[3].substring(5)), gen);
                        case "ACK" -> onAck(Integer.parseInt(t[1]), gen);
                        case "REJECT_STALE" -> onReject(Integer.parseInt(t[1]), t[2], gen);
                        case "NO_TASK" -> onNoTask(gen);
                        case "WELCOME" -> onWelcome(gen);
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

    // RING: Buddy에 연결하고 RING_OK
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

    // START: RUNNING으로 바꾸고 HB·Compute 시작
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
        onAssign(k, e, messageGeneration());
    }

    // ASSIGN: 로컬 Queue에 추가
    void onAssign(int k, int e, int gen) {
        qLock.lock();
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) return;
            localQ.add(new Item(k, e, 1, 0));
            requested = false;
            qCond.signalAll();
        } finally {
            stateLock.unlock();
            qLock.unlock();
        }
        log.ok("ASSIGN", "task=" + k + " e" + e + " received. range=[" + Primes.stepLo(k, 1) + ", " + (Primes.stepLo(k, 10) + 9999) + "]");
    }

    void onTakeover(int k, int e, int from) {
        onTakeover(k, e, from, messageGeneration());
    }

    // TAKEOVER: CKPT가 있으면 다음 step부터 Queue에 추가
    void onTakeover(int k, int e, int from, int gen) {
        Ckpt ck;
        qLock.lock();
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) return;
            storeLock.lock();
            try {
                ck = store.get(k);
            } finally {
                storeLock.unlock();
            }
            if (ck != null) localQ.add(new Item(k, e, ck.s + 1, ck.partial));
            else localQ.add(new Item(k, e, 1, 0));
            takeoverCount++;
            qCond.signalAll();
        } finally {
            stateLock.unlock();
            qLock.unlock();
        }
        if (ck != null)
            log.ok("TAKEOVER", String.format("task=%d e%d from W%d. ckpt=(e%d, step %d, %d) -> resume step %d",
                    k, e, from, ck.e, ck.s, ck.partial, ck.s + 1));
        else
            log.ok("TAKEOVER", String.format("task=%d e%d from W%d. no ckpt -> start step 1", k, e, from));
    }

    void onAck(int k) {
        onAck(k, messageGeneration());
    }

    // ACK 기록
    void onAck(int k, int gen) {
        boolean known;
        qLock.lock();
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) return;
            known = waitingAck.remove(k);
            if (known) ackCount++;
        } finally {
            stateLock.unlock();
            qLock.unlock();
        }
        if (!known) {
            log.info("COMMIT", "ACK " + k + " for unknown task. ignored");
            return;
        }
        log.ok("COMMIT", "ACK " + k + " (committed)");
    }

    void onReject(int k, String reason) {
        onReject(k, reason, messageGeneration());
    }

    // REJECT_STALE 기록
    void onReject(int k, String reason, int gen) {
        boolean known;
        qLock.lock();
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) return;
            known = waitingAck.remove(k);
            if (known) rejectCount++;
        } finally {
            stateLock.unlock();
            qLock.unlock();
        }
        if (!known) {
            log.info("REJECT_STALE", "REJECT_STALE " + k + " " + reason + " for unknown task. ignored");
            return;
        }
        log.warn("REJECT_STALE", "task=" + k + " rejected. reason=" + reason);
    }

    void onNoTask() {
        onNoTask(messageGeneration());
    }

    // NO_TASK: 이후 REQ 안 보냄
    void onNoTask(int gen) {
        qLock.lock();
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) return;
            noTask = true;
            requested = false;
        } finally {
            stateLock.unlock();
            qLock.unlock();
        }
        log.info("ASSIGN", "NO_TASK. no more REQ, waiting for TAKEOVER or BYE");
    }

    void onWelcome() {
        onWelcome(messageGeneration());
    }

    // WELCOME: RUNNING으로 돌아가고 REJOINED 전송
    void onWelcome(int gen) {
        stateLock.lock();
        try {
            if (!acceptsMessage(gen) || state != REJOINING) return;
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

    // RETRY_LATER: 2초 뒤 아직 REJOINING이면 REJOIN 재전송
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

    // BYE: 요약 로그 남기고 종료
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

    // Socket 닫기
    static void closeQuietly(Out o) {
        if (o == null) return;
        try {
            o.sock.close();
        } catch (IOException ignored) {
        }
    }

    // Peer 포트 연결 수락
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

    // 선행 Worker의 CKPT 연결 또는 Launcher 제어 연결 처리
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
                    int gen = messageGeneration();
                    String[] t = line.trim().split(" ");
                    if (!t[0].equals("CKPT")) continue;
                    try {
                        onCkpt(Integer.parseInt(t[1]), Integer.parseInt(t[2]), Integer.parseInt(t[3]), Integer.parseInt(t[4]), gen);
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
        }
    }

    void onCkpt(int k, int e, int s, int partial) {
        onCkpt(k, e, s, partial, messageGeneration());
    }

    // CKPT: (epoch, step)이 더 크면 Store에 저장
    void onCkpt(int k, int e, int s, int partial, int gen) {
        stateLock.lock();
        try {
            if (!acceptsMessage(gen)) {
                ckptDropped++;
                return;
            }
            ckptRecv++;
            ckptRecvWindow++;
            storeLock.lock();
            try {
                Ckpt old = store.get(k);
                if (old == null || e > old.e || (e == old.e && s > old.s)) store.put(k, new Ckpt(e, s, partial));
            } finally {
                storeLock.unlock();
            }
        } finally {
            stateLock.unlock();
        }
    }

    // CRASH: Queue·Store·플래그를 지우고 침묵
    void onCrash() {
        int qn, sn;
        stateLock.lock();
        try {
            if (state == CRASHED || crashPending) return;
            crashPending = true;
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

    // RESTART: REJOINING으로 바꾸고 REJOIN 전송
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

    // RUNNING일 때 1초마다 HB, 10초마다 CKPT 건수 로그
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

    // Queue에서 Task를 꺼냄, 비었으면 REQ 보내고 대기
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

    // step 계산, step마다 CKPT, 끝나면 RESULT
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
                stateLock.lock();
                try {
                    if (state != RUNNING || crashGen != gen) continue;
                    waitingAck.add(it.k);
                } finally {
                    stateLock.unlock();
                    qLock.unlock();
                }
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

    // 현재 crash 세대
    int crashGenNow() {
        stateLock.lock();
        try {
            return crashGen;
        } finally {
            stateLock.unlock();
        }
    }

    // CKPT 전송 수 세기
    void countCkptSent() {
        stateLock.lock();
        try {
            ckptSent++;
            ckptSentWindow++;
        } finally {
            stateLock.unlock();
        }
    }

    // 실행 인자 읽고 시작
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
