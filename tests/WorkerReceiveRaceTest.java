import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class WorkerReceiveRaceTest {
    static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    static Worker worker(Path logs, String name) throws Exception {
        Worker worker = new Worker(1, "127.0.0.1", 1, "127.0.0.1", 1, 0,
                logs.resolve(name + ".txt").toString());
        worker.state = Worker.RUNNING;
        return worker;
    }

    static void queuedMessagesCannotSurviveCrash(Path logs) throws Exception {
        Worker worker = worker(logs, "queued");
        int gen = worker.messageGeneration();
        Thread receiving = new Thread(() -> {
            worker.onAssign(271, 1, gen);
            worker.onTakeover(152, 2, 4, gen);
            worker.onNoTask(gen);
            worker.onAck(271, gen);
            worker.onReject(152, "EPOCH", gen);
        });
        receiving.setDaemon(true);
        worker.qLock.lock();
        try {
            receiving.start();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!worker.qLock.hasQueuedThread(receiving) && System.nanoTime() < end) Thread.sleep(1);
            check(worker.qLock.hasQueuedThread(receiving), "receiver did not wait for Queue");
            worker.onCrash();
            // Model a full rejoin while the old message handler is still delayed.
            worker.stateLock.lock();
            try { worker.state = Worker.REJOINING; }
            finally { worker.stateLock.unlock(); }
        } finally {
            worker.qLock.unlock();
        }
        receiving.join(3000);
        check(!receiving.isAlive(), "receiver deadlocked");
        check(worker.localQ.isEmpty() && !worker.noTask && !worker.requested,
                "old control message restored state after crash/rejoin");
        worker.onCkpt(152, 1, 6, 3642, gen);
        worker.onWelcome(gen);
        check(worker.store.isEmpty(), "old CKPT restored deleted data");
        check(worker.getState() == Worker.REJOINING, "old WELCOME started a new generation");

        int currentGen = worker.messageGeneration();
        worker.onCkpt(152, 1, 6, 3642, currentGen);
        worker.onCkpt(152, 1, 5, 3031, currentGen);
        check(worker.store.get(152).s == 6, "new-generation CKPT was dropped or regressed");
        worker.onCkpt(152, 2, 1, 599, currentGen);
        check(worker.store.get(152).e == 2 && worker.store.get(152).s == 1,
                "CKPT epoch ordering changed");
        worker.onWelcome(currentGen);
        worker.onAssign(271, 1, currentGen);
        worker.onTakeover(152, 3, 4, currentGen);
        check(worker.localQ.size() == 2, "current-generation control messages were rejected");
        worker.localQ.poll();
        Worker.Item resumed = worker.localQ.poll();
        check(resumed.startStep == 2 && resumed.partial == 599, "TAKEOVER did not resume the current CKPT");
        worker.log.close();
    }

    static void concurrentReceivesLeaveCrashEmpty(Path logs) throws Exception {
        for (int iteration = 0; iteration < 100; iteration++) {
            Worker worker = worker(logs, "stress-" + iteration);
            int gen = worker.messageGeneration();
            Thread control = new Thread(() -> {
                for (int k = 0; k < 30; k++) {
                    worker.onAssign(k, 1, gen);
                    worker.onTakeover(k, 2, 4, gen);
                    worker.onNoTask(gen);
                }
            });
            Thread peer = new Thread(() -> {
                for (int k = 0; k < 30; k++) worker.onCkpt(k, 1, 6, 3642, gen);
            });
            control.setDaemon(true);
            peer.setDaemon(true);
            control.start();
            peer.start();
            worker.onCrash();
            control.join(3000);
            peer.join(3000);
            check(!control.isAlive() && !peer.isAlive(), "receive/crash deadlocked");
            check(worker.localQ.isEmpty() && worker.store.isEmpty() && worker.waitingAck.isEmpty(),
                    "crash state repopulated in iteration " + iteration);
            check(!worker.requested && !worker.noTask, "crash flags restored in iteration " + iteration);
            worker.log.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Path logs = Files.createTempDirectory("hw3-receive-race-");
        queuedMessagesCannotSurviveCrash(logs);
        concurrentReceivesLeaveCrashEmpty(logs);
        System.out.println("PASS: delayed control/CKPT rejected across rejoin; current generation resumes; 100 crash races stay empty");
    }
}
