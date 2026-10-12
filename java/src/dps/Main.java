package dps;

import static dps.AppLogger.LOG;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;

/** Usage: java -cp out dps.Main [workers=4] [tasks=30] [outputCsv=output/java-results.csv] */
public class Main {

    public static void main(String[] args) {
        int workers = intArg(args, 0, 4);
        int taskCount = intArg(args, 1, 30);
        Path output = Path.of(args.length > 2 ? args[2] : "output/java-results.csv");

        AppLogger.init("logs/java-processing.log");
        System.exit(run(workers, taskCount, output));
    }

    private static int run(int workers, int taskCount, Path output) {
        long startNanos = System.nanoTime();
        SharedTaskQueue queue = new SharedTaskQueue();

        ResultStore opened;
        Path shownOutput = output;
        try {
            opened = new ResultStore(output);
        } catch (IOException e) {
            LOG.log(java.util.logging.Level.SEVERE, "cannot open output file " + output
                    + "; falling back to in-memory results only", e);
            opened = new ResultStore();
            shownOutput = null;
        }

        List<Worker.Stats> stats = new ArrayList<>();
        ExecutorService pool = null;
        try (ResultStore store = opened) {
            AtomicInteger counter = new AtomicInteger();
            pool = Executors.newFixedThreadPool(workers,
                    r -> new Thread(r, "worker-" + counter.incrementAndGet()));

            List<Future<Worker.Stats>> futures = new ArrayList<>();
            for (int i = 0; i < workers; i++) {
                futures.add(pool.submit(new Worker(queue, store)));
            }

            Thread producer = new Thread(() -> produce(queue, taskCount), "producer");
            producer.start();
            producer.join();

            pool.shutdown(); // accept no new jobs; workers exit when the queue is drained
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.severe("workers did not finish within 60s; forcing shutdown");
                pool.shutdownNow();
            }

            for (Future<Worker.Stats> f : futures) {
                try {
                    stats.add(f.get());
                } catch (ExecutionException e) {
                    LOG.log(java.util.logging.Level.SEVERE, "worker crashed", e.getCause());
                } catch (CancellationException e) {
                    LOG.severe("worker was cancelled");
                }
            }
            return summarize(store.snapshot(), stats, taskCount, shownOutput, startNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.severe("main thread interrupted; stopping workers");
            if (pool != null) {
                pool.shutdownNow();
            }
            return 2;
        }
    }

    private static void produce(SharedTaskQueue queue, int taskCount) {
        LOG.info("started; enqueueing " + taskCount + " tasks");
        try {
            for (int id = 1; id <= taskCount; id++) {
                queue.addTask(Task.generate(id));
                if (taskCount >= 4 && id == taskCount / 2) {
                    LOG.info("simulating slow upstream (pausing 900ms)");
                    Thread.sleep(900);
                }
            }
            LOG.info("all tasks enqueued");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("producer interrupted");
        } catch (IllegalStateException e) {
            LOG.severe("producer error: " + e.getMessage());
        } finally {
            queue.close(); // always close, or workers would wait forever
        }
    }

    private static int summarize(List<Result> results, List<Worker.Stats> stats, int expected,
                                 Path output, long startNanos) {
        results.sort(Comparator.comparingInt(Result::taskId));
        long ok = results.stream().filter(r -> r.status() == Result.Status.SUCCESS).count();
        boolean complete = results.size() == expected;
        for (int i = 0; i < results.size() && complete; i++) {
            complete = results.get(i).taskId() == i + 1; // no gaps, no duplicates
        }
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        LOG.info("---------------- SUMMARY ----------------");
        LOG.info("tasks submitted : " + expected);
        LOG.info("results saved   : " + results.size() + " (" + ok + " succeeded, "
                + (results.size() - ok) + " failed)");
        stats.stream().sorted(Comparator.comparing(Worker.Stats::name)).forEach(s ->
                LOG.info(s.name() + " handled " + (s.succeeded() + s.failed())
                        + " tasks, " + s.retries() + " retries"));
        LOG.info("integrity check : " + (complete ? "PASSED (no missing or duplicate tasks)" : "FAILED"));
        LOG.info("elapsed         : " + ms + " ms; output file: "
                + (output == null ? "none (in-memory only)" : output));
        return complete ? 0 : 1;
    }

    private static int intArg(String[] args, int index, int fallback) {
        if (args.length > index) {
            try {
                int v = Integer.parseInt(args[index]);
                if (v > 0) {
                    return v;
                }
            } catch (NumberFormatException ignored) {
                // fall through to default
            }
            System.err.println("Ignoring invalid argument '" + args[index] + "', using " + fallback);
        }
        return fallback;
    }
}
