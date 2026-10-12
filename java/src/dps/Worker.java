package dps;

import static dps.AppLogger.LOG;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** Pulls tasks from the shared queue until it is closed and empty, saving each result. */
public class Worker implements Callable<Worker.Stats> {

    public record Stats(String name, int succeeded, int failed, int retries) { }

    private static final int MAX_ATTEMPTS = 3;
    private static final long POLL_TIMEOUT_MS = 400;

    private final SharedTaskQueue queue;
    private final ResultStore store;

    private int succeeded;
    private int failed;
    private int retries;

    public Worker(SharedTaskQueue queue, ResultStore store) {
        this.queue = queue;
        this.store = store;
    }

    @Override
    public Stats call() {
        String name = Thread.currentThread().getName();
        LOG.info("started");
        try {
            while (true) {
                Optional<Task> next;
                try {
                    next = queue.getTask(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (EmptyQueueException e) {
                    LOG.warning("queue empty (" + e.getMessage() + "); still waiting");
                    continue;
                }
                if (next.isEmpty()) {
                    break; // queue closed and drained: normal termination
                }

                Result result;
                try {
                    result = process(next.get(), name);
                } catch (RuntimeException e) {
                    // One bad task must not kill the worker.
                    LOG.log(java.util.logging.Level.SEVERE, "unexpected error on task " + next.get().id(), e);
                    result = new Result(next.get().id(), name, next.get().payload(), "", 0, 1,
                            Result.Status.FAILED, e.toString());
                    failed++;
                }
                store.save(result);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // preserve interrupt status for the executor
            LOG.warning("interrupted; shutting down early");
        } finally {
            LOG.info("completed: succeeded=" + succeeded + ", failed=" + failed
                    + ", retries=" + retries);
        }
        return new Stats(name, succeeded, failed, retries);
    }

    private Result process(Task task, String workerName) throws InterruptedException {
        long start = System.nanoTime();
        for (int attempt = 1; ; attempt++) {
            try {
                String output = DataProcessor.process(task, attempt);
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                LOG.info("task " + task.id() + " OK (attempts=" + attempt + ", " + ms + "ms) -> "
                        + output.substring(output.indexOf("sha256=")));
                succeeded++;
                return new Result(task.id(), workerName, task.payload(), output, ms, attempt,
                        Result.Status.SUCCESS, "");
            } catch (ProcessingException e) {
                if (e.isTransient() && attempt < MAX_ATTEMPTS) {
                    retries++;
                    LOG.warning("task " + task.id() + " attempt " + attempt + " failed: "
                            + e.getMessage() + " - retrying");
                    continue;
                }
                long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                LOG.severe("task " + task.id() + " FAILED after " + attempt + " attempt(s): "
                        + e.getMessage());
                failed++;
                return new Result(task.id(), workerName, task.payload(), "", ms, attempt,
                        Result.Status.FAILED, e.getMessage());
            }
        }
    }
}
