import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Launcher {
    static final int RUNNING = 0, CRASHED = 1, REJOINING = 2, EXITED = 3;
    static final String[] STATUS = {"RUNNING", "CRASHED", "REJOINING", "EXITED"};
    static final double NO_CRASH_AFTER = 80.0;

    static class Event {
        int no, victim;
        double crashT, d, restartT = -1, rejoinedT = -1;
        String crashClock, restartClock = "-", rejoinedClock = "-";
        String down;
    }

    final int n;
    final String coord;
    final String peerIp;
    final int peerBase;
    final int stepDelay;
    final long seed;
    final String logDir;
    final boolean chaos;
    final boolean manual;
    final Random rng;
    final Log log;

    final ReentrantLock lock = new ReentrantLock();
    final Condition cond = lock.newCondition();
    final int[] status;
    final double[] restartAt;
    final Event[] openEvent;
    final Process[] procs;
    final Writer[] ctrl;
    final List<Event> events = new ArrayList<>();
    int readyCount = 0;
    long t0 = 0;
    double nextCrash = -1;
    boolean blocked = false;

    Launcher(int n, String coord, String peerIp, int peerBase, int stepDelay, long seed, String logDir,
             boolean chaos, boolean manual) throws IOException {
        this.n = n;
        this.coord = coord;
        this.peerIp = peerIp;
        this.peerBase = peerBase;
        this.stepDelay = stepDelay;
        this.seed = seed;
        this.logDir = logDir;
        this.chaos = chaos;
        this.manual = manual;
        rng = new Random(seed);
        log = new Log("LAUNCHER", logDir + "/Launcher.txt", true);
        status = new int[n + 1];
        restartAt = new double[n + 1];
        openEvent = new Event[n + 1];
        procs = new Process[n + 1];
        ctrl = new Writer[n + 1];
        for (int i = 1; i <= n; i++) restartAt[i] = -1;
    }

    double rel() {
        return (System.nanoTime() - t0) / 1e9;
    }

    double uniform(double lo, double hi) {
        return lo + (hi - lo) * rng.nextDouble();
    }

    String downList() {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (status[i] == CRASHED || status[i] == REJOINING) sb.append(sb.length() > 0 ? "," : "").append("W").append(i);
        }
        return "[" + sb + "]";
    }

    int downCount() {
        int c = 0;
        for (int i = 1; i <= n; i++) if (status[i] == CRASHED || status[i] == REJOINING) c++;
        return c;
    }

    void send(int i, String msg) {
        try {
            synchronized (ctrl[i]) {
                ctrl[i].write(msg + "\n");
                ctrl[i].flush();
            }
        } catch (IOException e) {
            log.warn("CONNECT", "send to W" + i + " failed: " + e.getMessage());
        }
    }

    void run() throws Exception {
        log.info("INIT", String.format("seed=%d workers=%d coord=%s peer=%s:%d+i step_delay=%dms chaos=%s manual=%s",
                seed, n, coord, peerIp, peerBase, stepDelay, chaos ? "on" : "off", manual ? "on" : "off"));
        startWorkers();
        for (int i = 1; i <= n; i++) connectWorker(i);

        lock.lock();
        try {
            while (readyCount < n) cond.await();
            t0 = System.nanoTime();
            if (chaos) nextCrash = uniform(10, 20);
        } finally {
            lock.unlock();
        }
        log.ok("START", "all READY (" + n + "/" + n + ") -> t=0" + (chaos ? String.format(". first crash at t=%.2f", nextCrash) : ". chaos off"));

        if (manual) {
            Thread t = new Thread(this::consoleLoop, "Console");
            t.setDaemon(true);
            t.start();
        }

        schedule();
        writeSummary();
        for (int i = 1; i <= n; i++) procs[i].waitFor(5, TimeUnit.SECONDS);
        log.ok("TERMINATE", "all workers EXITED. launcher exit");
        log.close();
    }

    void startWorkers() throws IOException {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String cp = System.getProperty("java.class.path");
        File errDir = new File(logDir, "err");
        errDir.mkdirs();
        for (int i = 1; i <= n; i++) {
            ProcessBuilder pb = new ProcessBuilder(java, "-cp", cp, "Worker",
                    "--id", String.valueOf(i), "--coord", coord, "--peer-ip", peerIp,
                    "--peer-port", String.valueOf(peerBase + i), "--step-delay", String.valueOf(stepDelay),
                    "--log-dir", logDir);
            pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
            pb.redirectError(new File(errDir, "Worker" + i + ".err"));
            procs[i] = pb.start();
            log.info("INIT", "W" + i + " started (pid " + procs[i].pid() + ", peer port " + (peerBase + i) + ")");
        }
    }

    void connectWorker(int i) throws InterruptedException, IOException {
        Socket s = Worker.connectRetry(peerIp, peerBase + i);
        ctrl[i] = new BufferedWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8));
        send(i, "LAUNCHER");
        log.ok("CONNECT", "control connection to W" + i);
        final Socket sock = s;
        Thread t = new Thread(() -> readWorker(i, sock), "Ctrl-W" + i);
        t.setDaemon(true);
        t.start();
    }

    void readWorker(int i, Socket s) {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("READY")) {
                    lock.lock();
                    try {
                        readyCount++;
                        cond.signalAll();
                    } finally {
                        lock.unlock();
                    }
                    log.info("START", "READY " + i);
                } else if (line.startsWith("REJOINED")) {
                    onRejoined(i);
                }
            }
        } catch (IOException ignored) {
        }
        String msg;
        lock.lock();
        try {
            status[i] = EXITED;
            msg = restartAt[i] >= 0 ? " (scheduled RESTART canceled)" : "";
            restartAt[i] = -1;
            cond.signalAll();
        } finally {
            lock.unlock();
        }
        log.info("TERMINATE", "W" + i + " control connection EOF -> EXITED" + msg);
    }

    void onRejoined(int i) {
        String msg;
        lock.lock();
        try {
            if (status[i] != REJOINING) return;
            status[i] = RUNNING;
            double now = rel();
            Event ev = openEvent[i];
            if (ev != null) {
                ev.rejoinedT = now;
                ev.rejoinedClock = Log.now();
                openEvent[i] = null;
            }
            msg = String.format("W%d t=%.2f down=%s", i, now, downList());
            if (blocked) {
                blocked = false;
                nextCrash = now + uniform(5, 15);
                if (nextCrash > NO_CRASH_AFTER) {
                    msg += String.format(". next crash candidate t=%.2f > %.0f -> no more crashes", nextCrash, NO_CRASH_AFTER);
                    nextCrash = -1;
                } else {
                    msg += String.format(". unblocked, next crash at t=%.2f", nextCrash);
                }
            }
            cond.signalAll();
        } finally {
            lock.unlock();
        }
        log.ok("REJOIN", "REJOINED " + msg);
    }

    // 주 스케줄 루프. RESTART 시각과 다음 CRASH 시각 중 가까운 쪽까지 CV로 잔다
    void schedule() throws InterruptedException {
        lock.lock();
        try {
            while (true) {
                boolean allExited = true;
                for (int i = 1; i <= n; i++) if (status[i] != EXITED) allExited = false;
                if (allExited) return;

                double now = rel();
                for (int i = 1; i <= n; i++) {
                    if (restartAt[i] >= 0 && now >= restartAt[i]) {
                        restartAt[i] = -1;
                        status[i] = REJOINING;
                        Event ev = openEvent[i];
                        if (ev != null) {
                            ev.restartT = now;
                            ev.restartClock = Log.now();
                        }
                        String msg = String.format("#%d W%d t=%.2f (down %.1fs)", ev != null ? ev.no : 0, i, now, ev != null ? now - ev.crashT : 0);
                        lock.unlock();
                        try {
                            send(i, "RESTART");
                            log.info("RESTART", msg);
                        } finally {
                            lock.lock();
                        }
                    }
                }

                if (chaos && !blocked && nextCrash >= 0 && now >= nextCrash) {
                    if (downCount() >= 2) {
                        blocked = true;
                        String msg = "2 workers down " + downList() + ". wait for REJOINED";
                        lock.unlock();
                        try {
                            log.info("CRASH", msg);
                        } finally {
                            lock.lock();
                        }
                    } else {
                        crashRandom(now);
                    }
                    continue;
                }

                double wake = Double.MAX_VALUE;
                for (int i = 1; i <= n; i++) if (restartAt[i] >= 0) wake = Math.min(wake, restartAt[i]);
                if (chaos && !blocked && nextCrash >= 0) wake = Math.min(wake, nextCrash);
                if (wake == Double.MAX_VALUE) cond.await();
                else {
                    long ns = (long) ((wake - rel()) * 1e9);
                    if (ns > 0) cond.awaitNanos(ns);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // lock을 잡은 상태로 호출됨
    void crashRandom(double now) {
        List<Integer> running = new ArrayList<>();
        for (int i = 1; i <= n; i++) if (status[i] == RUNNING) running.add(i);
        if (running.isEmpty()) {
            nextCrash = now + uniform(5, 15);
            return;
        }
        int victim = running.get(rng.nextInt(running.size()));
        double d = uniform(10, 15);
        status[victim] = CRASHED;
        restartAt[victim] = now + d;

        Event ev = new Event();
        ev.no = events.size() + 1;
        ev.victim = victim;
        ev.crashT = now;
        ev.d = d;
        ev.crashClock = Log.now();
        ev.down = downList();
        events.add(ev);
        openEvent[victim] = ev;

        nextCrash = now + uniform(5, 15);
        String tail;
        if (nextCrash > NO_CRASH_AFTER) {
            tail = String.format(". next crash candidate t=%.2f > %.0f -> no more crashes", nextCrash, NO_CRASH_AFTER);
            nextCrash = -1;
        } else {
            tail = String.format(". next crash at t=%.2f", nextCrash);
        }
        String msg = String.format("#%d victim=W%d (random from RUNNING=%s) D=%.1fs t=%.2f down=%s%s",
                ev.no, victim, running, d, now, ev.down, tail);
        lock.unlock();
        try {
            send(victim, "CRASH");
            log.info("CRASH", msg);
        } finally {
            lock.lock();
        }
    }

    // --manual: 콘솔에서 "crash 4", "restart 4" 입력 (개발용)
    void consoleLoop() {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        try {
            String line;
            while ((line = in.readLine()) != null) {
                String[] t = line.trim().split("\\s+");
                if (t.length != 2) continue;
                int i;
                try {
                    i = Integer.parseInt(t[1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                if (i < 1 || i > n) continue;
                if (t[0].equals("crash")) manualCrash(i);
                else if (t[0].equals("restart")) manualRestart(i);
            }
        } catch (IOException ignored) {
        }
    }

    void manualCrash(int i) {
        String msg;
        lock.lock();
        try {
            if (status[i] != RUNNING) {
                msg = null;
            } else {
                double now = rel();
                status[i] = CRASHED;
                Event ev = new Event();
                ev.no = events.size() + 1;
                ev.victim = i;
                ev.crashT = now;
                ev.d = -1;
                ev.crashClock = Log.now();
                ev.down = downList();
                events.add(ev);
                openEvent[i] = ev;
                msg = String.format("#%d victim=W%d (manual) t=%.2f down=%s", ev.no, i, now, ev.down);
            }
        } finally {
            lock.unlock();
        }
        if (msg == null) {
            log.warn("CRASH", "manual crash W" + i + " ignored (not RUNNING)");
            return;
        }
        send(i, "CRASH");
        log.info("CRASH", msg);
    }

    void manualRestart(int i) {
        String msg;
        lock.lock();
        try {
            if (status[i] != CRASHED) {
                msg = null;
            } else {
                double now = rel();
                status[i] = REJOINING;
                restartAt[i] = -1;
                Event ev = openEvent[i];
                if (ev != null) {
                    ev.restartT = now;
                    ev.restartClock = Log.now();
                    ev.d = now - ev.crashT;
                }
                msg = String.format("W%d (manual) t=%.2f", i, now);
            }
        } finally {
            lock.unlock();
        }
        if (msg == null) {
            log.warn("RESTART", "manual restart W" + i + " ignored (not CRASHED)");
            return;
        }
        send(i, "RESTART");
        log.info("RESTART", msg);
    }

    void writeSummary() {
        List<String> lines = new ArrayList<>();
        lines.add("===== LAUNCHER SUMMARY (seed=" + seed + ") =====");
        lines.add("#  victim crash_t  D      restart_t rejoined_t down_at_crash  crash(real)   restart(real) rejoined(real)");
        int maxDown = 0;
        lock.lock();
        try {
            for (Event ev : events) {
                lines.add(String.format("%-2d W%-5d %7.2f  %5.1f  %9s %10s %-14s %s  %-13s %s",
                        ev.no, ev.victim, ev.crashT, ev.d,
                        ev.restartT < 0 ? "-" : String.format("%.2f", ev.restartT),
                        ev.rejoinedT < 0 ? "-" : String.format("%.2f", ev.rejoinedT),
                        ev.down, ev.crashClock, ev.restartClock, ev.rejoinedClock));
                maxDown = Math.max(maxDown, ev.down.split(",").length);
            }
            lines.add("total CRASH=" + events.size() + ", max concurrent down=" + maxDown);
        } finally {
            lock.unlock();
        }
        for (String l : lines) log.info("TERMINATE", l);
    }

    public static void main(String[] args) throws Exception {
        String coord = "127.0.0.1:8000";
        int workers = 10;
        String peerIp = "127.0.0.1";
        int peerBase = 7000;
        int stepDelay = 100;
        long seed = 42;
        String logDir = "logs";
        boolean chaos = true, manual = false;
        for (int a = 0; a < args.length; a++) {
            switch (args[a]) {
                case "--coord" -> coord = args[++a];
                case "--workers" -> workers = Integer.parseInt(args[++a]);
                case "--peer-ip" -> peerIp = args[++a];
                case "--peer-base" -> peerBase = Integer.parseInt(args[++a]);
                case "--step-delay" -> stepDelay = Integer.parseInt(args[++a]);
                case "--seed" -> seed = Long.parseLong(args[++a]);
                case "--log-dir" -> logDir = args[++a];
                case "--no-chaos" -> chaos = false;
                case "--manual" -> manual = true;
                default -> {
                    System.out.println("usage: java Launcher --coord host:port [--workers 10] [--peer-ip 127.0.0.1] [--peer-base 7000]");
                    System.out.println("                     [--step-delay 100] [--seed 42] [--log-dir logs] [--no-chaos] [--manual]");
                    return;
                }
            }
        }
        new Launcher(workers, coord, peerIp, peerBase, stepDelay, seed, logDir, chaos, manual).run();
    }
}
