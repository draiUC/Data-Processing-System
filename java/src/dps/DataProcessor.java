package dps;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;

/** The "computational work": sleeps to simulate load, then transforms and fingerprints the payload. */
public final class DataProcessor {

    private DataProcessor() { }

    public static String process(Task task, int attempt) throws ProcessingException, InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(80, 221)); // simulated work

        // Deterministic simulated faults so error handling is visible in every run.
        if (task.id() % 10 == 0) {
            throw new ProcessingException("corrupt record (checksum mismatch)", false);
        }
        if (task.id() % 7 == 0 && attempt == 1) {
            throw new ProcessingException("transient I/O timeout", true);
        }

        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(task.payload().getBytes(StandardCharsets.UTF_8));
            return task.payload().toUpperCase() + "|sha256=" + HexFormat.of().formatHex(digest, 0, 4);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
