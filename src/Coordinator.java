import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Coordinator {
    static final int N = 10;
    static final int TASKS = 1000;
    static final int DISPATCHERS = 6;
    static final long EXPECTED_SUM = 5761455;

    static final int PENDING = 0, ASSIGNED = 1, DONE = 2;
    static final int ALIVE = 0, SUSPECT = 1, DEAD = 2;

    static class Task {
        final ReentrantLock lock = new ReentrantLock();
        int state = PENDING;
        int owner = 0;
        int epoch = 0;
        int result = -1;
        int commitCount = 0;
    }

    static class Conn {
        final SocketChannel ch;
        final ReentrantLock sendLock = new ReentrantLock();
        final StringBuilder buf = new StringBuilder();
        volatile int id = 0;

        Conn(SocketChannel ch) { this.ch = ch; }
    }

    static class WorkerEntry {
        int state = ALIVE;
        long lastHB;
        int rejoinCount = 0;
        int hbWindow = 0;
        Conn conn;
        String peerIp;
        int peerPort;
        boolean ringOk = false;
    }

    static class Req {
        final Conn conn;
        final String line;

        Req(Conn conn, String line) { this.conn = conn; this.line = line; }
    }

    static class CrashRec {
        int no, worker, to;
        double lastHB, dead;
        List<Integer> moved = new ArrayList<>();
        Set<Integer> waiting = new HashSet<>();
        double recoverDone = -1, rejoin = -1, firstAssign = -1;
        int firstTask = -1;
        int commitsAfterRejoin = 0;
    }

    final Task[] tasks = new Task[TASKS];
    final WorkerEntry[] workers = new WorkerEntry[N + 1];
    final ReentrantLock wtLock = new ReentrantLock();
    final Condition setupCond = wtLock.newCondition();
    final ArrayDeque<Integer> pending = new ArrayDeque<>();
    int registered = 0;
    int ringOkCount = 0;
    boolean started = false;

    final ArrayDeque<Req> reqQueue = new ArrayDeque<>();
    final ReentrantLock reqLock = new ReentrantLock();
    final Condition notEmpty = reqLock.newCondition();

    final ReentrantLock doneLock = new ReentrantLock();
    final Condition allDone = doneLock.newCondition();
    int doneCount = 0;
    boolean finished = false;
    long total = 0;
    long lastCommitNanos = 0;

    final ReentrantLock ctlLock = new ReentrantLock();
    final Condition ctlCond = ctlLock.newCondition();
    volatile boolean stopping = false;
    volatile boolean judging = true;
    volatile boolean byeSent = false;

    final ReentrantLock metricLock = new ReentrantLock();
    final List<CrashRec> crashes = new ArrayList<>();
    final CrashRec[] lastCrash = new CrashRec[N + 1];
    int deadTotal = 0, rejoinTotal = 0, takeoverTotal = 0, rejectTotal = 0, maxConcurrentDead = 0;

    volatile long startNanos = 0;
    final Log log;
    final String host;
    final int port;
    Selector selector;
    ServerSocketChannel server;

    Coordinator(String host, int port, String logPath) throws IOException {
        this.host = host;
        this.port = port;
        log = new Log("COORD", logPath, true);
        for (int k = 0; k < TASKS; k++) {
            tasks[k] = new Task();
            pending.add(k);
        }
        for (int i = 1; i <= N; i++) workers[i] = new WorkerEntry();
    }

    // START 기준 경과 초
    double rel(long nanos) {
        return (nanos - startNanos) / 1e9;
    }

    // 지금의 START 기준 경과 초
    double relNow() {
        return rel(System.nanoTime());
    }

    // Worker에게 한 줄 전송
    void send(Conn c, String msg) {
        if (c == null) return;
        ByteBuffer b = StandardCharsets.UTF_8.encode(msg + "\n");
        c.sendLock.lock();
        try {
            while (b.hasRemaining()) c.ch.write(b);
        } catch (IOException e) {
            log.warn("CONNECT", "W" + c.id + " send failed: " + msg + " (" + e.getMessage() + ")");
        } finally {
            c.sendLock.unlock();
        }
    }

    // Worker 번호로 연결 찾기
    Conn connOf(int i) {
        wtLock.lock();
        try {
            return workers[i].conn;
        } finally {
            wtLock.unlock();
        }
    }

    // Request Queue에 넣고 Dispatcher 하나를 깨움
    void put(Req r) {
        reqLock.lock();
        try {
            reqQueue.add(r);
            notEmpty.signal();
        } finally {
            reqLock.unlock();
        }
    }

    // Request Queue에서 하나 꺼냄 (비었으면 대기)
    Req take() throws InterruptedException {
        reqLock.lock();
        try {
            while (reqQueue.isEmpty()) notEmpty.await();
            return reqQueue.poll();
        } finally {
            reqLock.unlock();
        }
    }


    // Thread 시작, Ring 구성과 START, 종료 대기 후 VERIFY·BYE·join
    void run() throws Exception {
        selector = Selector.open();
        server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(host, port));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
        log.ok("INIT", "listening on " + host + ":" + port + ", waiting for " + N + " workers");

        List<Thread> threads = new ArrayList<>();
        threads.add(new Thread(this::listenerLoop, "Listener"));
        for (int n = 1; n <= DISPATCHERS; n++) {
            final int id = n;
            threads.add(new Thread(() -> dispatcherLoop(id), "D" + n));
        }
        threads.add(new Thread(this::fdLoop, "FD"));
        threads.add(new Thread(this::monitorLoop, "Monitor"));
        for (Thread t : threads) t.start();

        String[] ringMsg = new String[N + 1];
        Conn[] conns = new Conn[N + 1];
        wtLock.lock();
        try {
            while (registered < N) setupCond.await();
            for (int i = 1; i <= N; i++) {
                int j = i % N + 1;
                ringMsg[i] = "RING " + j + " " + workers[j].peerIp + " " + workers[j].peerPort;
                conns[i] = workers[i].conn;
            }
        } finally {
            wtLock.unlock();
        }
        for (int i = 1; i <= N; i++) send(conns[i], ringMsg[i]);
        log.ok("RING", "10/10 HELLO. RING sent (Wi -> W(i mod 10 + 1))");

        wtLock.lock();
        try {
            while (ringOkCount < N) setupCond.await();
            startNanos = System.nanoTime();
            for (int i = 1; i <= N; i++) workers[i].lastHB = startNanos;
            started = true;
        } finally {
            wtLock.unlock();
        }
        for (int i = 1; i <= N; i++) send(conns[i], "START");
        log.ok("START", "ring ready (10/10 RING_OK). START sent to 10 workers");
        ctlLock.lock();
        try {
            ctlCond.signalAll();
        } finally {
            ctlLock.unlock();
        }

        doneLock.lock();
        try {
            while (!finished) allDone.await();
        } finally {
            doneLock.unlock();
        }

        judging = false;
        verify();

        byeSent = true;
        log.info("TERMINATE", "send BYE to 10 workers");
        for (int i = 1; i <= N; i++) send(conns[i], "BYE");
        stopping = true;

        for (int n = 0; n < DISPATCHERS; n++) put(new Req(null, null));
        ctlLock.lock();
        try {
            ctlCond.signalAll();
        } finally {
            ctlLock.unlock();
        }
        selector.wakeup();
        for (Thread t : threads) t.join();
        for (int i = 1; i <= N; i++) {
            try {
                if (conns[i] != null) conns[i].ch.close();
            } catch (IOException ignored) {
            }
        }
        server.close();
        selector.close();
        log.ok("TERMINATE", "all threads joined. coordinator exit");
        log.close();
    }

    // 1000개 DONE, commit_count, 합계 검증
    void verify() {
        long sum = 0;
        int done = 0, commits = 0, dbl = 0;
        for (Task t : tasks) {
            t.lock.lock();
            try {
                if (t.state == DONE) {
                    done++;
                    sum += t.result;
                }
                commits += t.commitCount;
                if (t.commitCount >= 2) dbl++;
            } finally {
                t.lock.unlock();
            }
        }
        long runningTotal;
        doneLock.lock();
        try {
            runningTotal = total;
        } finally {
            doneLock.unlock();
        }
        boolean pass = done == TASKS && commits == TASKS && dbl == 0 && sum == EXPECTED_SUM && runningTotal == sum;
        String msg = String.format("done=%d commits=%d double=%d sum=%d (running total=%d) %s",
                done, commits, dbl, sum, runningTotal, pass ? "PASS" : "FAIL");
        if (pass) log.ok("VERIFY", msg);
        else log.fail("VERIFY", msg);
        printSummary(sum, done, commits, dbl, pass);
    }

    // 최종 요약과 crash별 지표 출력
    void printSummary(long sum, int done, int commits, int dbl, boolean pass) {
        double elapsed = rel(lastCommitNanos);
        List<String> lines = new ArrayList<>();
        lines.add("===== HW#3 RESULT =====");
        lines.add(String.format("Elapsed : %.1f s (START -> last COMMIT)  Throughput : %.2f task/s", elapsed, TASKS / elapsed));
        lines.add(String.format("Tasks   : done=%d commits=%d double_commit=%d", done, commits, dbl));
        lines.add(String.format("Sum     : %d (expected %d) -> %s", sum, EXPECTED_SUM, pass ? "PASS" : "FAIL"));
        metricLock.lock();
        try {
            lines.add(String.format("Crashes : DEAD=%d REJOIN=%d TAKEOVER=%d REJECT_STALE=%d max_concurrent_dead=%d",
                    deadTotal, rejoinTotal, takeoverTotal, rejectTotal, maxConcurrentDead));
            lines.add("# worker lastHB  DEAD    detect  takeover(to)     recover  rejoin  1st_task      after_rejoin");
            for (CrashRec c : crashes) {
                String moved = c.moved.isEmpty() ? "-" : joinInts(c.moved) + "->W" + c.to;
                String rec = c.moved.isEmpty() ? "-" : (c.recoverDone < 0 ? "?" : String.format("%.1fs", c.recoverDone - c.dead));
                String rj = c.rejoin < 0 ? "-" : String.format("%.2f", c.rejoin);
                String first = c.firstTask < 0 ? "-" : String.format("%d (%.2fs)", c.firstTask, c.firstAssign - c.rejoin);
                lines.add(String.format("%-2d W%-4d %6.2f  %6.2f  %5.2fs  %-16s %-8s %-7s %-13s %d",
                        c.no, c.worker, c.lastHB, c.dead, c.dead - c.lastHB, moved, rec, rj, first, c.commitsAfterRejoin));
            }
        } finally {
            metricLock.unlock();
        }
        for (String l : lines) log.info("VERIFY", l);
    }

    // 숫자 목록을 쉼표로 이어 붙임
    static String joinInts(List<Integer> list) {
        StringBuilder sb = new StringBuilder();
        for (int x : list) {
            if (sb.length() > 0) sb.append(',');
            sb.append(x);
        }
        return sb.toString();
    }


    // select로 Socket 감시
    void listenerLoop() {
        ByteBuffer rb = ByteBuffer.allocate(8192);
        try {
            while (!stopping) {
                selector.select(500);
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;
                    try {
                        handleKey(key, rb);
                    } catch (IOException e) {
                        key.cancel();
                        Object att = key.attachment();
                        if (att instanceof Conn c) {
                            try {
                                c.ch.close();
                            } catch (IOException ignored) {
                            }
                            if (!byeSent) log.warn("CONNECT", "W" + c.id + " connection error: " + e.getMessage());
                        } else {
                            log.warn("CONNECT", "accept failed: " + e.getMessage());
                        }
                    }
                }
            }
        } catch (IOException e) {
            if (!stopping) log.fail("CONNECT", "listener error: " + e.getMessage());
        }
    }

    // 연결 수락, 수신 데이터를 줄 단위로 잘라 HB는 직접, 나머지는 Queue로
    void handleKey(SelectionKey key, ByteBuffer rb) throws IOException {
        if (key.isAcceptable()) {
            SocketChannel ch = server.accept();
            if (ch == null) return;
            ch.configureBlocking(false);
            ch.register(selector, SelectionKey.OP_READ, new Conn(ch));
            log.info("CONNECT", "new connection from " + ch.getRemoteAddress());
        } else if (key.isReadable()) {
            Conn c = (Conn) key.attachment();
            rb.clear();
            int n = c.ch.read(rb);
            if (n < 0) {
                key.cancel();
                c.ch.close();
                if (!byeSent) log.warn("CONNECT", "W" + c.id + " connection closed (EOF)");
                else log.info("TERMINATE", "W" + c.id + " closed after BYE");
                return;
            }
            rb.flip();
            c.buf.append(StandardCharsets.UTF_8.decode(rb));
            int nl;
            while ((nl = c.buf.indexOf("\n")) >= 0) {
                String line = c.buf.substring(0, nl).trim();
                c.buf.delete(0, nl + 1);
                if (line.isEmpty()) continue;
                if (line.startsWith("HB ")) onHB(c);
                else put(new Req(c, line));
            }
        }
    }

    // lastHB 갱신, SUSPECT면 ALIVE로
    void onHB(Conn c) {
        int i = c.id;
        if (i == 0) return;
        boolean back = false;
        wtLock.lock();
        try {
            WorkerEntry w = workers[i];
            if (w.state == DEAD) return;
            w.lastHB = System.nanoTime();
            w.hbWindow++;
            if (w.state == SUSPECT) {
                w.state = ALIVE;
                back = true;
            }
        } finally {
            wtLock.unlock();
        }
        if (back) log.info("HB", "W" + i + " HB received again. SUSPECT->ALIVE");
    }


    // Dispatcher: Queue에서 꺼내 처리
    void dispatcherLoop(int id) {
        try {
            while (true) {
                Req r = take();
                if (r.conn == null) break;
                try {
                    handle(r.conn, r.line);
                } catch (RuntimeException e) {
                    log.warn("CONNECT", "D" + id + " bad message from W" + r.conn.id + ": " + r.line);
                }
            }
        } catch (InterruptedException ignored) {
        }
    }

    // 메시지 종류별로 처리 함수 호출
    void handle(Conn c, String line) {
        String[] t = line.split(" ");
        switch (t[0]) {
            case "HELLO" -> onHello(c, Integer.parseInt(t[1]), t[2], Integer.parseInt(t[3]));
            case "RING_OK" -> onRingOk(c);
            case "REQ" -> onReq(c);
            case "RESULT" -> onResult(c, Integer.parseInt(t[1]), Integer.parseInt(t[2]), Integer.parseInt(t[3]));
            case "REJOIN" -> onRejoin(c);
            default -> log.warn("CONNECT", "unknown message from W" + c.id + ": " + line);
        }
    }

    // HELLO: Worker 등록
    void onHello(Conn c, int i, String ip, int peerPort) {
        String err = null;
        wtLock.lock();
        try {
            if (started) err = "after START";
            else if (i < 1 || i > N) err = "out of range";
            else if (workers[i].conn != null || c.id != 0) err = "duplicate";
            else {
                workers[i].conn = c;
                workers[i].peerIp = ip;
                workers[i].peerPort = peerPort;
                c.id = i;
                registered++;
                if (registered == N) setupCond.signalAll();
            }
        } finally {
            wtLock.unlock();
        }
        if (err != null) log.warn("CONNECT", "HELLO " + i + " ignored (" + err + ")");
        else log.ok("CONNECT", "W" + i + " registered. peer=" + ip + ":" + peerPort);
    }

    // RING_OK 개수 세기
    void onRingOk(Conn c) {
        int i = c.id;
        if (i == 0) return;
        wtLock.lock();
        try {
            if (!workers[i].ringOk) {
                workers[i].ringOk = true;
                ringOkCount++;
                if (ringOkCount == N) setupCond.signalAll();
            }
        } finally {
            wtLock.unlock();
        }
        log.ok("RING", "W" + i + " RING_OK (buddy connected)");
    }

    // REQ: DEAD 확인과 PENDING 배정을 한 번에 하고 ASSIGN 또는 NO_TASK
    void onReq(Conn c) {
        int i = c.id;
        if (i == 0) return;
        int k = -1;
        String warn = null;
        wtLock.lock();
        try {
            if (!started) warn = "REQ before START";
            else if (workers[i].state == DEAD) warn = "REQ from DEAD W" + i;
            else {
                Integer next = pending.poll();
                if (next != null) {
                    k = next;
                    Task task = tasks[k];
                    task.lock.lock();
                    try {
                        task.state = ASSIGNED;
                        task.owner = i;
                        task.epoch = 1;
                    } finally {
                        task.lock.unlock();
                    }
                }
            }
        } finally {
            wtLock.unlock();
        }
        if (warn != null) {
            log.warn("ASSIGN", warn + " ignored");
            return;
        }
        if (k < 0) {
            send(c, "NO_TASK");
            log.info("ASSIGN", "no pending task. NO_TASK -> W" + i);
            return;
        }
        send(c, "ASSIGN " + k + " 1");
        boolean firstAfterRejoin = false;
        metricLock.lock();
        try {
            CrashRec cr = lastCrash[i];
            if (cr != null && cr.rejoin >= 0 && cr.firstTask < 0) {
                cr.firstTask = k;
                cr.firstAssign = relNow();
                firstAfterRejoin = true;
            }
        } finally {
            metricLock.unlock();
        }
        log.ok("ASSIGN", "task=" + k + " e1 -> W" + i + (firstAfterRejoin ? " (first task after rejoin)" : ""));
    }

    // RESULT: DEAD/DONE/EPOCH 확인 후 Commit 또는 REJECT_STALE
    void onResult(Conn c, int k, int e, int v) {
        int sender = c.id;
        if (sender == 0) return;
        if (k < 0 || k >= TASKS) {
            log.warn("RESULT", "bad task number " + k + " from W" + sender);
            return;
        }
        boolean senderDead;
        wtLock.lock();
        try {
            senderDead = workers[sender].state == DEAD;
        } finally {
            wtLock.unlock();
        }
        String reason = null;
        int curEpoch, curOwner, cc = 0;
        Task task = tasks[k];
        if (senderDead) {
            reason = "DEAD";
            curEpoch = -1;
            curOwner = -1;
        } else {
            task.lock.lock();
            try {
                curEpoch = task.epoch;
                curOwner = task.owner;
                if (task.state == DONE) reason = "DONE";
                else if (task.epoch != e || task.owner != sender) reason = "EPOCH";
                else {
                    task.state = DONE;
                    task.result = v;
                    task.commitCount++;
                    cc = task.commitCount;
                }
            } finally {
                task.lock.unlock();
            }
        }

        if (reason != null) {
            send(c, "REJECT_STALE " + k + " " + reason);
            metricLock.lock();
            try {
                rejectTotal++;
            } finally {
                metricLock.unlock();
            }
            String detail = reason.equals("EPOCH") ? String.format(" (current e%d owner W%d)", curEpoch, curOwner) : "";
            log.warn("REJECT_STALE", "task=" + k + " e" + e + " from W" + sender + " value=" + v + " reason=" + reason + detail);
            return;
        }

        int count;
        doneLock.lock();
        try {
            doneCount++;
            total += v;
            count = doneCount;
            lastCommitNanos = System.nanoTime();
        } finally {
            doneLock.unlock();
        }
        send(c, "ACK " + k);

        double now = relNow();
        metricLock.lock();
        try {
            for (CrashRec cr : crashes) {
                if (cr.waiting.remove(k) && cr.waiting.isEmpty()) cr.recoverDone = now;
            }
            CrashRec mine = lastCrash[sender];
            if (mine != null && mine.rejoin >= 0) mine.commitsAfterRejoin++;
        } finally {
            metricLock.unlock();
        }
        log.ok("COMMIT", "task=" + k + " e" + e + " by W" + sender + " value=" + v + " commit_count=" + cc + " (done " + count + "/1000)");

        if (count == TASKS) {
            doneLock.lock();
            try {
                finished = true;
                allDone.signal();
            } finally {
                doneLock.unlock();
            }
        }
    }

    // REJOIN: DEAD면 ALIVE로 바꾸고 WELCOME, 아니면 RETRY_LATER
    void onRejoin(Conn c) {
        int i = c.id;
        if (i == 0) return;
        boolean ok;
        int cnt;
        wtLock.lock();
        try {
            ok = workers[i].state == DEAD;
            if (ok) {
                workers[i].state = ALIVE;
                workers[i].lastHB = System.nanoTime();
                workers[i].rejoinCount++;
            }
            cnt = workers[i].rejoinCount;
        } finally {
            wtLock.unlock();
        }
        if (!ok) {
            send(c, "RETRY_LATER");
            log.warn("REJOIN", "W" + i + " is not DEAD yet. RETRY_LATER");
            return;
        }
        metricLock.lock();
        try {
            rejoinTotal++;
            if (lastCrash[i] != null) lastCrash[i].rejoin = relNow();
        } finally {
            metricLock.unlock();
        }
        log.ok("REJOIN", "W" + i + " DEAD->ALIVE rejoin_count=" + cnt + ". WELCOME");
        send(c, "WELCOME");
    }


    // ms만큼 대기 (종료 시 바로 깨어남)
    boolean waitTick(long ms) {
        ctlLock.lock();
        try {
            long left = TimeUnit.MILLISECONDS.toNanos(ms);
            while (!stopping && left > 0) left = ctlCond.awaitNanos(left);
            return !stopping;
        } catch (InterruptedException e) {
            return false;
        } finally {
            ctlLock.unlock();
        }
    }

    // START 될 때까지 대기
    boolean waitStart() {
        ctlLock.lock();
        try {
            while (!stopping && startNanos == 0) ctlCond.await();
            return !stopping;
        } catch (InterruptedException e) {
            return false;
        } finally {
            ctlLock.unlock();
        }
    }

    // d 다음 순서의 첫 ALIVE (없으면 첫 SUSPECT)
    int nextAlive(int d) {
        for (int j = 1; j < N; j++) {
            int b = (d - 1 + j) % N + 1;
            if (workers[b].state == ALIVE) return b;
        }
        for (int j = 1; j < N; j++) {
            int b = (d - 1 + j) % N + 1;
            if (workers[b].state == SUSPECT) return b;
        }
        return -1;
    }

    // d가 owner인 ASSIGNED Task를 b에게 넘김 (epoch + 1)
    List<Integer> moveTasks(int d, int b, List<int[]> moved) {
        List<Integer> owned = new ArrayList<>();
        for (int k = 0; k < TASKS; k++) {
            Task task = tasks[k];
            task.lock.lock();
            try {
                if (task.state == ASSIGNED && task.owner == d) {
                    owned.add(k);
                    if (b > 0) {
                        task.epoch++;
                        task.owner = b;
                        moved.add(new int[]{b, k, task.epoch, d});
                    }
                }
            } finally {
                task.lock.unlock();
            }
        }
        return owned;
    }

    // Failure Detector: 1초마다 SUSPECT/DEAD 판정과 TAKEOVER
    void fdLoop() {
        if (!waitStart()) return;
        while (waitTick(1000)) {
            if (!judging) continue;
            List<String> suspectLogs = new ArrayList<>();
            List<String> deadLogs = new ArrayList<>();
            List<int[]> moved = new ArrayList<>();
            List<CrashRec> newRecs = new ArrayList<>();
            int deadNow = 0;
            long now = System.nanoTime();

            wtLock.lock();
            try {
                List<Integer> newlyDead = new ArrayList<>();
                for (int i = 1; i <= N; i++) {
                    WorkerEntry w = workers[i];
                    if (w.state == DEAD) continue;
                    double gap = (now - w.lastHB) / 1e9;
                    if (gap >= 5) newlyDead.add(i);
                    else if (gap >= 3 && w.state == ALIVE) {
                        w.state = SUSPECT;
                        suspectLogs.add(String.format("W%d no HB for %.2fs", i, gap));
                    }
                }
                for (int d : newlyDead) workers[d].state = DEAD;
                List<Integer> deadList = new ArrayList<>();
                for (int i = 1; i <= N; i++) if (workers[i].state == DEAD) deadList.add(i);
                deadNow = deadList.size();

                for (int d : newlyDead) {
                    int b = nextAlive(d);
                    List<Integer> owned = moveTasks(d, b, moved);
                    double gap = (now - workers[d].lastHB) / 1e9;
                    CrashRec cr = new CrashRec();
                    cr.worker = d;
                    cr.to = b;
                    cr.lastHB = rel(workers[d].lastHB);
                    cr.dead = rel(now);
                    cr.moved.addAll(owned);
                    cr.waiting.addAll(owned);
                    newRecs.add(cr);
                    StringBuilder dn = new StringBuilder();
                    for (int x : deadList) dn.append(dn.length() > 0 ? "," : "").append("W").append(x);
                    deadLogs.add(String.format("W%d no HB for %.2fs (lastHB=%.2f). owned=[%s] next_alive=%s dead_now=[%s]",
                            d, gap, cr.lastHB, joinInts(owned), b > 0 ? "W" + b : "none", dn));
                }
            } finally {
                wtLock.unlock();
            }

            for (String l : suspectLogs) log.warn("SUSPECT", l);
            for (String l : deadLogs) log.info("DEAD", l);
            for (int[] m : moved) {
                Conn c = connOf(m[0]);
                send(c, "TAKEOVER " + m[1] + " " + m[2] + " from=" + m[3]);
                log.ok("TAKEOVER", String.format("task=%d epoch %d->%d owner W%d->W%d", m[1], m[2] - 1, m[2], m[3], m[0]));
            }
            if (!newRecs.isEmpty() || !moved.isEmpty()) {
                metricLock.lock();
                try {
                    for (CrashRec cr : newRecs) {
                        cr.no = crashes.size() + 1;
                        crashes.add(cr);
                        lastCrash[cr.worker] = cr;
                        deadTotal++;
                    }
                    takeoverTotal += moved.size();
                    maxConcurrentDead = Math.max(maxConcurrentDead, deadNow);
                } finally {
                    metricLock.unlock();
                }
            }
        }
    }


    // 5초마다 POOL과 HB 요약 로그
    void monitorLoop() {
        if (!waitStart()) return;
        while (waitTick(5000)) {
            int queueLen, pend, done;
            StringBuilder alive = new StringBuilder();
            StringBuilder hb = new StringBuilder();
            reqLock.lock();
            try {
                queueLen = reqQueue.size();
            } finally {
                reqLock.unlock();
            }
            wtLock.lock();
            try {
                pend = pending.size();
                long now = System.nanoTime();
                for (int i = 1; i <= N; i++) {
                    WorkerEntry w = workers[i];
                    if (w.state == ALIVE) alive.append(alive.length() > 0 ? "," : "").append(i);
                    if (hb.length() > 0) hb.append(' ');
                    hb.append(String.format("W%d=%d(%s,%.1fs)", i, w.hbWindow,
                            w.state == ALIVE ? "A" : w.state == SUSPECT ? "S" : "D", (now - w.lastHB) / 1e9));
                    w.hbWindow = 0;
                }
            } finally {
                wtLock.unlock();
            }
            doneLock.lock();
            try {
                done = doneCount;
            } finally {
                doneLock.unlock();
            }
            log.info("POOL", String.format("queue=%d pending=%d assigned=%d done=%d alive=[%s]",
                    queueLen, pend, TASKS - pend - done, done, alive));
            log.info("HB", "last 5s: " + hb);
        }
    }

    // 실행 인자 읽고 시작
    public static void main(String[] args) throws Exception {
        String host = "0.0.0.0";
        int port = 8000;
        String logPath = "logs/Coordinator.txt";
        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "--host" -> host = args[++a];
                case "--port" -> port = Integer.parseInt(args[++a]);
                case "--log" -> logPath = args[++a];
                default -> {
                    System.out.println("usage: java Coordinator [--host 0.0.0.0] [--port 8000] [--log logs/Coordinator.txt]");
                    return;
                }
            }
        }
        new Coordinator(host, port, logPath).run();
    }
}
