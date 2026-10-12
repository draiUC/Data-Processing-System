package dps;

import static dps.AppLogger.LOG;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Shared output resource: an in-memory result list plus an optional CSV file.
 * One lock protects both, so a result is never half-written or written twice.
 */
public class ResultStore implements AutoCloseable {
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Result> results = new ArrayList<>();
    private final Set<Integer> seenIds = new HashSet<>();
    private BufferedWriter writer; // null when running in memory-only mode

    /** Memory-only store (used as a fallback when the output file cannot be opened). */
    public ResultStore() {
        this.writer = null;
    }

    /** File-backed store. @throws IOException if the directory or file cannot be created. */
    public ResultStore(Path file) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
        this.writer.write(Result.CSV_HEADER);
        this.writer.newLine();
    }

    public void save(Result result) {
        lock.lock();
        try {
            if (!seenIds.add(result.taskId())) {
                LOG.warning("duplicate result for task " + result.taskId() + " ignored");
                return;
            }
            results.add(result);
            if (writer != null) {
                try {
                    writer.write(result.toCsv());
                    writer.newLine();
                    writer.flush();
                } catch (IOException e) {
                    // Keep the in-memory copy; stop writing to the broken file.
                    LOG.log(java.util.logging.Level.SEVERE,
                            "file write failed for task " + result.taskId()
                                    + "; continuing in memory only", e);
                    closeQuietly();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Copy of the results so far (safe to read while workers are still running). */
    public List<Result> snapshot() {
        lock.lock();
        try {
            return new ArrayList<>(results);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closeQuietly();
        } finally {
            lock.unlock();
        }
    }

    private void closeQuietly() {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException e) {
            LOG.warning("error closing output file: " + e.getMessage());
        } finally {
            writer = null;
        }
    }
}
