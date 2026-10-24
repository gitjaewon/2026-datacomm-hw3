import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerSendRaceTest {
    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static void awaitQueued(java.util.concurrent.locks.ReentrantLock lock, Thread thread) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!lock.hasQueuedThread(thread) && System.nanoTime() < end) Thread.sleep(1);
        check(lock.hasQueuedThread(thread), "sender did not wait for its socket");
    }

    static class DelayedOut extends Worker.Out {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        DelayedOut(Socket socket) throws IOException { super(socket); }

        @Override void send(String message) throws IOException {
            entered.countDown();
            try {
                if (!release.await(3, TimeUnit.SECONDS)) throw new IOException("test send timed out");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            super.send(message);
        }
    }

    static Worker worker(Path logs) throws IOException {
        Worker worker = new Worker(1, "127.0.0.1", 1, "127.0.0.1", 1, 0,
                logs.resolve("worker.txt").toString());
        worker.state = Worker.RUNNING;
        return worker;
    }

    static void queuedSendIsDiscarded(Path logs) throws Exception {
        Worker worker = worker(logs);
        try (ServerSocket server = new ServerSocket(0);
             Socket sender = new Socket("127.0.0.1", server.getLocalPort());
             Socket receiver = server.accept()) {
            receiver.setSoTimeout(200);
            Worker.Out out = new Worker.Out(sender);
            worker.coord = out;
            AtomicBoolean sent = new AtomicBoolean(true);
            out.lock.lock();
            Thread thread = new Thread(() -> sent.set(worker.sendIfAlive(0, out, "HB 1")));
            thread.setDaemon(true);
            try {
                thread.start();
                awaitQueued(out.lock, thread);
                worker.onCrash();
            } finally {
                out.lock.unlock();
            }
            thread.join(3000);
            check(!thread.isAlive() && !sent.get(), "queued send survived CRASH");
            // Even after rejoin, the previous generation must stay invalid.
            worker.state = Worker.RUNNING;
            check(!worker.sendIfAlive(0, out, "RESULT 0 1 9592"), "old generation sent after rejoin");
            try {
                receiver.getInputStream().read();
                throw new AssertionError("CRASHED worker wrote to the socket");
            } catch (SocketTimeoutException expected) {
                // The connection stays open, with no message written.
            }
        } finally {
            worker.log.close();
        }
    }

    static void crashDrainsInFlightSend(Path logs) throws Exception {
        Worker worker = worker(logs);
        try (ServerSocket server = new ServerSocket(0);
             Socket sender = new Socket("127.0.0.1", server.getLocalPort());
             Socket receiver = server.accept()) {
            receiver.setSoTimeout(3000);
            DelayedOut out = new DelayedOut(sender);
            worker.coord = out;
            AtomicBoolean sent = new AtomicBoolean();
            Thread sending = new Thread(() -> sent.set(worker.sendIfAlive(0, out, "HB 1")));
            Thread crashing = new Thread(worker::onCrash);
            sending.setDaemon(true);
            crashing.setDaemon(true);
            sending.start();
            check(out.entered.await(3, TimeUnit.SECONDS), "send did not start");
            check(worker.stateLock.tryLock(1, TimeUnit.SECONDS), "stateLock held during socket IO");
            worker.stateLock.unlock();
            crashing.start();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            boolean pending = false;
            while (!pending && System.nanoTime() < end) {
                worker.stateLock.lock();
                try { pending = worker.crashPending; }
                finally { worker.stateLock.unlock(); }
                if (!pending) Thread.sleep(1);
            }
            check(pending && crashing.isAlive(), "CRASH did not wait for the in-flight send");
            out.release.countDown();
            sending.join(3000);
            crashing.join(3000);
            check(!sending.isAlive() && !crashing.isAlive(), "send/crash deadlocked");
            check(sent.get() && worker.getState() == Worker.CRASHED, "CRASH did not finish after send");
            check("HB 1".equals(new BufferedReader(new InputStreamReader(receiver.getInputStream())).readLine()),
                    "approved message was not delivered");
            check(!worker.sendIfAlive(worker.crashGen, out, "HB 1"), "send accepted while CRASHED");
        } finally {
            worker.log.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Path logs = Files.createTempDirectory("hw3-send-race-");
        queuedSendIsDiscarded(logs);
        crashDrainsInFlightSend(logs);
        System.out.println("PASS: queued send discarded; old generation rejected; in-flight send drained without stateLock IO");
    }
}
