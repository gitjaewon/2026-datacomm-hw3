import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Log {
    static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final String node;
    private final PrintWriter out;
    private final boolean console;

    Log(String node, String path, boolean console) throws IOException {
        this.node = node;
        this.console = console;
        File f = new File(path);
        if (f.getParentFile() != null) f.getParentFile().mkdirs();
        out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8));
    }

    // 현재 시각 (KST)
    static String now() {
        return ZonedDateTime.now(KST).format(TIME);
    }

    // 로그 한 줄 기록
    synchronized void write(String event, String status, String msg) {
        String line = String.format("[%s] %s | %-8s | %-7s | %s", now(), node, event, status, msg);
        out.println(line);
        out.flush();
        if (console) System.out.println(line);
    }

    void info(String event, String msg) { write(event, "INFO", msg); }
    void ok(String event, String msg) { write(event, "SUCCESS", msg); }
    void warn(String event, String msg) { write(event, "WARN", msg); }
    void fail(String event, String msg) { write(event, "FAIL", msg); }

    // 파일 닫기
    synchronized void close() {
        out.close();
    }
}
