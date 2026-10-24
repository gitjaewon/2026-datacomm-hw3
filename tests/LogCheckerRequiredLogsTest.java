import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LogCheckerRequiredLogsTest {
    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static String line(String node, String event, String message) {
        return "[12:00:00.000] " + node + " | " + event + " | SUCCESS | " + message + "\n";
    }

    static Map<String, String> completeLogs() {
        Map<String, String> files = new LinkedHashMap<>();
        StringBuilder coord = new StringBuilder();
        StringBuilder[] workers = new StringBuilder[10];
        for (int i = 0; i < workers.length; i++) workers[i] = new StringBuilder(line("WORKER" + (i + 1), "INIT", "worker start"));
        long sum = 0;
        for (int k = 0; k < 1000; k++) {
            int value = 0;
            for (int s = 1; s <= 10; s++) value += Primes.count(k, s);
            sum += value;
            int worker = k % 10;
            coord.append(line("COORD", "COMMIT", "task=" + k + " e1 by W" + (worker + 1) + " value=" + value));
            // ACK may be fewer than COMMIT if a crash happens just after a Commit.
            if (k < 999) workers[worker].append(line("WORKER" + (worker + 1), "COMMIT", "ACK " + k + " (committed)"));
        }
        check(sum == 5761455, "fixture prime sum is wrong");
        coord.append(line("COORD", "VERIFY", "done=1000 commits=1000 double=0 sum=" + sum + " PASS"));
        files.put("Coordinator.txt", coord.toString());
        files.put("Launcher.txt", line("LAUNCHER", "INIT", "launcher start"));
        for (int i = 0; i < workers.length; i++) files.put("Worker" + (i + 1) + ".txt", workers[i].toString());
        return files;
    }

    static String run(LogChecker checker, Path dir, boolean expected) throws Exception {
        PrintStream original = System.out;
        try (PrintStream quiet = new PrintStream(OutputStream.nullOutputStream())) {
            System.setOut(quiet);
            checker.run(dir);
        } finally {
            System.setOut(original);
        }
        String result = Files.readString(dir.resolve("check_result.txt"), StandardCharsets.UTF_8);
        check(checker.allPass == expected, "unexpected result for " + dir + "\n" + result);
        check(result.contains(expected ? "RESULT: ALL PASS" : "RESULT: FAIL"), "result was not written");
        if (!expected) {
            check(!result.contains("ALL PASS"), "incomplete logs reported ALL PASS");
            check(!result.contains("total CKPT saved"), "incomplete logs were used to calculate CKPT savings");
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("hw3-required-logs-");
        Map<String, String> files = completeLogs();
        for (Map.Entry<String, String> entry : files.entrySet())
            Files.writeString(dir.resolve(entry.getKey()), entry.getValue(), StandardCharsets.UTF_8);
        LogChecker checker = new LogChecker();
        run(checker, dir, true);
        for (Map.Entry<String, String> entry : files.entrySet()) {
            Path file = dir.resolve(entry.getKey());
            Files.delete(file);
            check(run(checker, dir, false).contains(entry.getKey()), "missing filename was not reported");
            Files.writeString(file, entry.getValue(), StandardCharsets.UTF_8);
        }
        Path worker = dir.resolve("Worker1.txt");
        for (String invalid : new String[]{"", "not a log entry\n", line("WORKER2", "INIT", "wrong worker")}) {
            Files.writeString(worker, invalid, StandardCharsets.UTF_8);
            run(checker, dir, false);
        }
        Files.writeString(worker, files.get("Worker1.txt"), StandardCharsets.UTF_8);
        run(checker, dir, true); // Reusing a checker must clear its previous failure.
        Path missing = dir.resolve("missing-directory");
        run(checker, missing, false);
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "LogChecker", missing.toString())
                .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        check(process.waitFor(10, TimeUnit.SECONDS), "checker CLI hung");
        check(process.exitValue() != 0, "incomplete logs returned exit code 0");
        System.out.println("PASS: complete logs including ACK<1000; each of 12 missing files; empty/invalid/wrong NODE; failed CLI exit");
    }
}
