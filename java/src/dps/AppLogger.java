package dps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

/** Central logging setup: every line shows time, level and the name of the logging thread. */
public final class AppLogger {
    public static final Logger LOG = Logger.getLogger("dps");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private AppLogger() { }

    public static void init(String logFile) {
        LOG.setUseParentHandlers(false);
        LOG.setLevel(Level.ALL);

        Formatter formatter = new Formatter() {
            @Override
            public String format(LogRecord r) {
                String level = switch (r.getLevel().getName()) {
                    case "SEVERE" -> "ERROR";
                    case "WARNING" -> "WARN";
                    default -> "INFO";
                };
                // Handlers call format() on the logging thread, so the thread name is accurate.
                String line = String.format("%s %-5s [%s] %s%n", LocalTime.now().format(TIME),
                        level, Thread.currentThread().getName(), formatMessage(r));
                if (r.getThrown() != null) {
                    line += "    caused by: " + r.getThrown() + System.lineSeparator();
                }
                return line;
            }
        };

        StreamHandler console = new StreamHandler(System.out, formatter) {
            @Override
            public synchronized void publish(LogRecord record) {
                super.publish(record);
                flush();
            }
        };
        LOG.addHandler(console);

        try {
            Files.createDirectories(Path.of(logFile).getParent());
            FileHandler file = new FileHandler(logFile, false);
            file.setFormatter(formatter);
            LOG.addHandler(file);
        } catch (IOException | SecurityException e) {
            LOG.warning("Cannot open log file " + logFile + " (" + e.getMessage()
                    + "); logging to console only");
        }
    }
}
