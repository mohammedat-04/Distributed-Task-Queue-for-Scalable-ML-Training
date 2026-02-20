package de.luh.vss.chat.dtq.metrics;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Locale;

/**
 * Lightweight CSV logger for experiment metrics.
 * All files are written under the local {@code logs/} directory to avoid external deps.
 */
public final class ExperimentLogger {

    private static final Path LOG_DIR = Paths.get("logs");

    private ExperimentLogger() {}

    /**
     * Appends per-epoch metrics for a job.
     */
    public static void logEpoch(String jobId,
                                int epoch,
                                double loss,
                                double accuracy,
                                long durationMs,
                                double throughput,
                                int workerCount,
                                String mode,
                                String weightsCsv) {
        Path file = LOG_DIR.resolve(jobId + "_epochs.csv");
        String header = "timestamp,jobId,epoch,loss,accuracy,durationMs,throughput,workers,mode\n";
        String line = String.format(Locale.US,
                "%s,%s,%d,%.6f,%.6f,%d,%.3f,%d,%s%n",
                Instant.now().toString(), jobId, epoch, loss,
                accuracy, durationMs, throughput, workerCount, mode);
        appendWithHeader(file, header, line);

        // Optionally persist weights snapshot per epoch for later analysis/plots.
        if (weightsCsv != null && !weightsCsv.isEmpty()) {
            Path wFile = LOG_DIR.resolve(jobId + String.format("_epoch%03d_weights.csv", epoch));
            writeString(wFile, weightsCsv);
        }
    }

    /**
     * Appends a job-level summary: total wall time and throughput.
     */
    public static void logJobSummary(String jobId,
                                     long wallMs,
                                     double throughput,
                                     int workerCount,
                                     String mode) {
        Path file = LOG_DIR.resolve("job_summaries.csv");
        String header = "timestamp,jobId,wallMs,throughput,workers,mode\n";
        String line = String.format(Locale.US,
                "%s,%s,%d,%.3f,%d,%s%n",
                Instant.now().toString(), jobId, wallMs, throughput, workerCount, mode);
        appendWithHeader(file, header, line);
    }

    /**
     * Records a speedup observation for plotting later.
     */
    public static void logSpeedupPoint(String jobId,
                                       int workerCount,
                                       String mode,
                                       long wallMs) {
        Path file = LOG_DIR.resolve("speedup.csv");
        String header = "timestamp,jobId,workers,mode,wallMs\n";
        String line = String.format(Locale.US,
                "%s,%s,%d,%s,%d%n",
                Instant.now().toString(), jobId, workerCount, mode, wallMs);
        appendWithHeader(file, header, line);
    }

    private static void appendWithHeader(Path file, String header, String line) {
        try {
            Files.createDirectories(LOG_DIR);
            if (Files.notExists(file)) {
                Files.writeString(file, header, StandardCharsets.UTF_8);
            }
            Files.writeString(file, line, StandardCharsets.UTF_8,
                    java.nio.file.StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("ExperimentLogger failed to write " + file + ": " + e.getMessage());
        }
    }

    private static void writeString(Path file, String content) {
        try {
            Files.createDirectories(LOG_DIR);
            Files.writeString(file, content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("ExperimentLogger failed to write " + file + ": " + e.getMessage());
        }
    }
}
