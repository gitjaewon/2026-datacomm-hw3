import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogChecker {
    static final Pattern LINE = Pattern.compile("^\\[(\\d\\d):(\\d\\d):(\\d\\d)\\.(\\d{3})\\] (\\S+) \\| (\\S+)\\s*\\| (\\S+)\\s*\\| (.*)$");
    static final Pattern COMMIT = Pattern.compile("task=(\\d+) e(\\d+) by W(\\d+) value=(\\d+)");
    static final Pattern DEAD = Pattern.compile("W(\\d+) no HB for ([\\d.]+)s .*owned=\\[([\\d,]*)\\] next_alive=(\\S+) dead_now=\\[([^\\]]*)\\]");
    static final Pattern TAKEOVER = Pattern.compile("task=(\\d+) epoch (\\d+)->(\\d+) owner W(\\d+)->W(\\d+)");
    static final Pattern REJOIN = Pattern.compile("W(\\d+) DEAD->ALIVE rejoin_count=(\\d+)");
    static final Pattern ASSIGN = Pattern.compile("task=(\\d+) e1 -> W(\\d+)");
    static final Pattern L_CRASH = Pattern.compile("#(\\d+) victim=W(\\d+)");
    static final Pattern W_TAKEOVER = Pattern.compile("task=(\\d+) e(\\d+) from W(\\d+)\\. (?:ckpt=.*-> resume step (\\d+)|no ckpt -> start step 1)");

    static class L {
        long ms;
        String node, event, status, msg;
    }

    static class Crash {
        int no, worker;
        long launcherCrash = -1, dead = -1, rejoin = -1, assign = -1;
        int assignTask = -1;
        List<int[]> moved = new ArrayList<>();
    }

    final List<String> out = new ArrayList<>();
    boolean allPass = true;

    // 로그 파일 읽기
    static List<L> read(Path p) throws IOException {
        List<L> list = new ArrayList<>();
        long dayOffset = 0, prev = 0;
        for (String s : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            Matcher m = LINE.matcher(s);
            if (!m.matches()) continue;
            L l = new L();
            l.ms = ((Long.parseLong(m.group(1)) * 60 + Long.parseLong(m.group(2))) * 60 + Long.parseLong(m.group(3))) * 1000
                    + Long.parseLong(m.group(4)) + dayOffset;
            if (l.ms < prev - 12 * 3600_000L) {
                dayOffset += 24 * 3600_000L;
                l.ms += 24 * 3600_000L;
            }
            prev = l.ms;
            l.node = m.group(5);
            l.event = m.group(6);
            l.status = m.group(7);
            l.msg = m.group(8);
            list.add(l);
        }
        return list;
    }

    // 검사 결과 한 줄 기록
    void check(String name, boolean ok, String detail) {
        out.add(String.format("[%s] %s : %s", ok ? "PASS" : "FAIL", name, detail));
        if (!ok) allPass = false;
    }

    // 로그 12개로 정합성 검사와 crash별 지표 계산
    void run(Path dir) throws IOException {
        out.clear();
        allPass = true;
        out.add("log dir: " + dir.toAbsolutePath());
        Map<String, List<L>> logs = new HashMap<>();
        List<String> problems = new ArrayList<>();
        Map<String, String> required = new LinkedHashMap<>();
        required.put("Coordinator.txt", "COORD");
        required.put("Launcher.txt", "LAUNCHER");
        for (int i = 1; i <= 10; i++) required.put("Worker" + i + ".txt", "WORKER" + i);
        for (Map.Entry<String, String> entry : required.entrySet()) {
            Path file = dir.resolve(entry.getKey());
            if (!Files.isRegularFile(file)) {
                problems.add(entry.getKey() + " missing or not a regular file");
                continue;
            }
            try {
                List<L> lines = read(file);
                if (lines.isEmpty()) problems.add(entry.getKey() + " empty or no valid log entries");
                else if (lines.stream().anyMatch(line -> !line.node.equals(entry.getValue())))
                    problems.add(entry.getKey() + " contains an unexpected NODE");
                else logs.put(entry.getKey(), lines);
            } catch (IOException e) {
                problems.add(entry.getKey() + " unreadable: " + e.getMessage());
            }
        }
        check("0. required logs (12 files)", problems.isEmpty(),
                problems.isEmpty() ? "all logs present with valid NODE entries" : String.join("; ", problems));
        if (!problems.isEmpty()) {
            out.add("CKPT savings not calculated: required logs are incomplete.");
            writeResult(dir);
            return;
        }
        List<L> coord = logs.get("Coordinator.txt");
        List<L> launcher = logs.get("Launcher.txt");
        Map<Integer, List<L>> workers = new HashMap<>();
        for (int i = 1; i <= 10; i++) workers.put(i, logs.get("Worker" + i + ".txt"));

        Map<Integer, Integer> commitTimes = new HashMap<>();
        long sum = 0;
        int commitLines = 0;
        for (L l : coord) {
            if (!l.event.equals("COMMIT")) continue;
            Matcher m = COMMIT.matcher(l.msg);
            if (!m.find()) continue;
            commitLines++;
            int k = Integer.parseInt(m.group(1));
            commitTimes.merge(k, 1, Integer::sum);
            sum += Long.parseLong(m.group(4));
        }
        int dbl = 0;
        for (int c : commitTimes.values()) if (c >= 2) dbl++;
        check("1. 1000 tasks DONE, sum of commit_count = 1000", commitTimes.size() == 1000 && dbl == 0,
                "distinct tasks committed=" + commitTimes.size() + ", double commit=" + dbl);
        check("2. sum = 5761455", sum == 5761455, "sum of COMMIT values=" + sum);

        List<Crash> crashes = new ArrayList<>();
        Map<Integer, Crash> openByWorker = new HashMap<>();
        int maxDead = 0, rejoinCount = 0;
        for (L l : coord) {
            if (l.event.equals("DEAD")) {
                Matcher m = DEAD.matcher(l.msg);
                if (!m.find()) continue;
                Crash c = new Crash();
                c.no = crashes.size() + 1;
                c.worker = Integer.parseInt(m.group(1));
                c.dead = l.ms;
                crashes.add(c);
                openByWorker.put(c.worker, c);
                String dn = m.group(5).trim();
                maxDead = Math.max(maxDead, dn.isEmpty() ? 0 : dn.split(",").length);
            } else if (l.event.equals("TAKEOVER")) {
                Matcher m = TAKEOVER.matcher(l.msg);
                if (!m.find()) continue;
                Crash c = openByWorker.get(Integer.parseInt(m.group(4)));
                if (c != null) c.moved.add(new int[]{Integer.parseInt(m.group(1)), Integer.parseInt(m.group(3)), Integer.parseInt(m.group(5))});
            } else if (l.event.equals("REJOIN") && l.status.equals("SUCCESS")) {
                Matcher m = REJOIN.matcher(l.msg);
                if (!m.find()) continue;
                rejoinCount++;
                Crash c = openByWorker.get(Integer.parseInt(m.group(1)));
                if (c != null && c.rejoin < 0) c.rejoin = l.ms;
            } else if (l.event.equals("ASSIGN") && l.msg.contains("first task after rejoin")) {
                Matcher m = ASSIGN.matcher(l.msg);
                if (!m.find()) continue;
                Crash c = openByWorker.get(Integer.parseInt(m.group(2)));
                if (c != null && c.assign < 0) {
                    c.assign = l.ms;
                    c.assignTask = Integer.parseInt(m.group(1));
                }
            }
        }

        List<L> lcrash = new ArrayList<>();
        for (L l : launcher) if (l.event.equals("CRASH") && L_CRASH.matcher(l.msg).find()) lcrash.add(l);
        check("3. launcher CRASH = coordinator DEAD = REJOIN",
                lcrash.size() == crashes.size() && crashes.size() == rejoinCount,
                "CRASH=" + lcrash.size() + ", DEAD=" + crashes.size() + ", REJOIN=" + rejoinCount);
        boolean[] used = new boolean[crashes.size()];
        for (L l : lcrash) {
            Matcher m = L_CRASH.matcher(l.msg);
            m.find();
            int w = Integer.parseInt(m.group(2));
            for (int j = 0; j < crashes.size(); j++) {
                Crash c = crashes.get(j);
                if (!used[j] && c.worker == w && c.dead >= l.ms) {
                    used[j] = true;
                    c.launcherCrash = l.ms;
                    break;
                }
            }
        }

        check("4. DEAD at any moment <= 2", maxDead <= 2, "max dead_now=" + maxDead);

        int noTaskAfter = 0, missing = 0;
        for (Crash c : crashes) {
            if (c.rejoin < 0) missing++;
            else if (c.assign < 0) {
                boolean noTask = false;
                for (L l : coord)
                    if (l.ms >= c.rejoin && l.event.equals("ASSIGN") && l.msg.contains("NO_TASK -> W" + c.worker)) noTask = true;
                if (noTask) noTaskAfter++;
                else missing++;
            }
        }
        check("5. new task after every rejoin", missing == 0,
                "crashes=" + crashes.size() + ", new task after rejoin="
                        + (crashes.size() - missing - noTaskAfter) + ", missing=" + missing + (noTaskAfter > 0 ? " (" + noTaskAfter + " had no task left)" : ""));

        int ackSum = 0;
        for (List<L> wl : workers.values())
            for (L l : wl) if (l.event.equals("COMMIT") && l.msg.contains("(committed)")) ackSum++;
        check("6. coordinator COMMIT = 1000 (worker ACK <= 1000)", commitLines == 1000 && ackSum <= 1000,
                "COMMIT lines=" + commitLines + ", worker ACK=" + ackSum);

        boolean verifyPass = false;
        for (L l : coord) if (l.event.equals("VERIFY") && l.msg.contains(" PASS")) verifyPass = true;
        check("Coordinator VERIFY", verifyPass, verifyPass ? "PASS line found" : "no PASS line");

        Map<String, Integer> resume = new HashMap<>();
        for (Map.Entry<Integer, List<L>> e : workers.entrySet()) {
            for (L l : e.getValue()) {
                if (!l.event.equals("TAKEOVER")) continue;
                Matcher m = W_TAKEOVER.matcher(l.msg);
                if (!m.find()) continue;
                int step = m.group(4) == null ? 1 : Integer.parseInt(m.group(4));
                resume.put(m.group(1) + "/" + m.group(2), step);
            }
        }
        out.add("");
        out.add("crash cycles (real time, (gap from previous stage))");
        out.add("#  W    CRASH(launcher)  DEAD                TAKEOVER                 REJOIN              ASSIGN after rejoin        CKPT saved");
        int savedTotal = 0, movedTotal = 0;
        for (Crash c : crashes) {
            int saved = 0;
            StringBuilder mv = new StringBuilder();
            for (int[] x : c.moved) {
                Integer st = resume.get(x[0] + "/" + x[1]);
                int s = st == null ? 1 : st;
                saved += s - 1;
                if (mv.length() > 0) mv.append(',');
                mv.append(x[0]).append("->W").append(x[2]).append("(s").append(s).append(')');
            }
            savedTotal += saved;
            movedTotal += c.moved.size();
            String ratio = c.moved.isEmpty() ? "-" : saved + "/" + (c.moved.size() * 10);
            out.add(String.format("%-2d W%-3d %-16s %-19s %-24s %-19s %-26s %s",
                    c.no, c.worker, clock(c.launcherCrash), withGap(c.dead, c.launcherCrash),
                    mv.length() == 0 ? "-" : mv.toString(), withGap(c.rejoin, c.dead),
                    c.assign < 0 ? "-" : clock(c.assign) + " task " + c.assignTask, ratio));
        }
        out.add(String.format("total CKPT saved = %d/%d", savedTotal, movedTotal * 10));
        writeResult(dir);
    }

    // 결과 출력, check_result.txt 저장
    void writeResult(Path dir) throws IOException {
        out.add("");
        out.add(allPass ? "RESULT: ALL PASS" : "RESULT: FAIL");
        for (String s : out) System.out.println(s);
        Files.createDirectories(dir);
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(dir.resolve("check_result.txt"), StandardCharsets.UTF_8))) {
            for (String s : out) pw.println(s);
        }
    }

    // ms를 HH:MM:SS.mmm로
    static String clock(long ms) {
        if (ms < 0) return "-";
        return String.format("%02d:%02d:%02d.%03d", ms / 3600000, ms / 60000 % 60, ms / 1000 % 60, ms % 1000);
    }

    // 시각과 이전 단계와의 간격
    static String withGap(long ms, long prev) {
        if (ms < 0) return "-";
        if (prev < 0) return clock(ms);
        return clock(ms) + String.format(" (%.2fs)", (ms - prev) / 1000.0);
    }

    // 로그 폴더를 받아 검사
    public static void main(String[] args) throws IOException {
        LogChecker checker = new LogChecker();
        checker.run(Path.of(args.length > 0 ? args[0] : "logs"));
        if (!checker.allPass) System.exit(1);
    }
}
