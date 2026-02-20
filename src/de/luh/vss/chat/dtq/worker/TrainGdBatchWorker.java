package de.luh.vss.chat.dtq.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Computes per-batch gradients for linear/logistic regression and caches CSV datasets.
 */
public final class TrainGdBatchWorker {

    private static final ConcurrentHashMap<String, CachedDataset> DATASET_CACHE = new ConcurrentHashMap<>();

    /**
     * Progress listener for long running tasks.
     */
    @FunctionalInterface
    public interface ProgressListener {
        void onProgress(int processed, int total);
    }

    /**
     * Creates a TrainGdBatchWorker.
     */
    private TrainGdBatchWorker() {
    }

    /**
     * Computes the batch loss and gradient for the given task payload.
     */
    public static TaskResult computeGradient(TaskPayload payload) {
        return computeGradient(payload, 0L);
    }

    /**
     * Computes the batch loss and gradient with an optional timeout (ms).
     */
    public static TaskResult computeGradient(TaskPayload payload, long maxDurationMs) {
        return computeGradient(payload, maxDurationMs, null);
    }

    /**
     * Computes the batch loss and gradient with optional timeout and progress callback.
     */
    public static TaskResult computeGradient(TaskPayload payload, long maxDurationMs, ProgressListener listener) {
        if (payload == null) {
            throw new IllegalArgumentException("payload is null");
        }
        int features = payload.getFeatures();
        if (features <= 0) {
            throw new IllegalArgumentException("features must be > 0");
        }
        if (payload.getStart() < 0) {
            throw new IllegalArgumentException("start must be >= 0");
        }
        if (payload.getCount() < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        double[] w = payload.getW();
        if (w == null || w.length != features) {
            throw new IllegalArgumentException("weights length must match features");
        }
        String model = payload.getModel();
        boolean isLogreg = "logreg".equalsIgnoreCase(model);
        String dataRef = payload.getDataRef();
        if (dataRef == null || dataRef.trim().isEmpty()) {
            throw new IllegalArgumentException("dataRef is empty");
        }

        Path path = resolveDataRef(dataRef);
        double[] gradSum = new double[features];
        double lossSum = 0.0;
        int rowsRead = 0;
        boolean hasHeader = payload.getHasHeader();
        String csvLabel = payload.getCsvLabel();
        double normalize = payload.getNormalize();
        if (normalize <= 0.0) {
            normalize = 1.0;
        }

        CachedDataset dataset = getCachedDataset(path, features, hasHeader, csvLabel, normalize);
        int start = payload.getStart();
        int end = Math.min(start + payload.getCount(), dataset.rows);
        if (start < 0 || start >= end) {
            return new TaskResult(payload.getJobId(), payload.getEpoch(), payload.getBatchId(),
                    0, 0.0, gradSum, payload.getAttempt());
        }
        long startedAt = System.currentTimeMillis();
        long lastReport = startedAt;
        for (int row = start; row < end; row++) {
            if (maxDurationMs > 0 && System.currentTimeMillis() - startedAt > maxDurationMs) {
                throw new RuntimeException("TASK_TIMEOUT");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new RuntimeException("TASK_INTERRUPTED");
            }
            double yHat = 0.0;
            int base = row * features;
            for (int i = 0; i < features; i++) {
                double xi = dataset.features[base + i];
                yHat += xi * w[i];
            }
            double y = mapLabelForTask(payload.getTask(), dataset.labels[row]);
            double error;
            if (isLogreg) {
                double pred = sigmoid(yHat);
                error = pred - y;
                double clamped = Math.min(1.0 - 1e-12, Math.max(1e-12, pred));
                lossSum += -((y * Math.log(clamped)) + ((1.0 - y) * Math.log(1.0 - clamped)));
            } else {
                error = yHat - y;
                lossSum += error * error;
            }
            for (int i = 0; i < features; i++) {
                gradSum[i] += dataset.features[base + i] * error;
            }
            rowsRead++;
            if (listener != null) {
                long now = System.currentTimeMillis();
                if ((rowsRead % 1024 == 0) || (now - lastReport) >= 1000) {
                    listener.onProgress(rowsRead, end - start);
                    lastReport = now;
                }
            }
        }
        if (listener != null && rowsRead > 0) {
            listener.onProgress(rowsRead, end - start);
        }

        if (rowsRead == 0 && payload.getCount() > 0) {
            System.err.println("No data read from " + path + " start=" + payload.getStart() +
                    " count=" + payload.getCount());
        }
        return new TaskResult(payload.getJobId(), payload.getEpoch(), payload.getBatchId(),
                rowsRead, lossSum, gradSum, payload.getAttempt());
    }

    /**
     * Returns cached Dataset.
     */
    private static CachedDataset getCachedDataset(Path path,
                                                  int features,
                                                  boolean hasHeader,
                                                  String csvLabel,
                                                  double normalize) {
        String key = buildCacheKey(path, features, hasHeader, csvLabel, normalize);
        long lastModified = getLastModified(path);
        CachedDataset cached = DATASET_CACHE.get(key);
        if (cached != null && cached.lastModified == lastModified) {
            return cached;
        }
        return DATASET_CACHE.compute(key, (ignored, existing) -> {
            if (existing != null && existing.lastModified == lastModified) {
                return existing;
            }
            return loadDataset(path, features, hasHeader, csvLabel, normalize, lastModified);
        });
    }

    /**
     * Loads dataset.
     */
    private static CachedDataset loadDataset(Path path,
                                             int features,
                                             boolean hasHeader,
                                             String csvLabel,
                                             double normalize,
                                             long lastModified) {
        int capacity = 1024;
        double[] featureData = new double[capacity * features];
        double[] labelData = new double[capacity];
        int rows = 0;
        int lineNumber = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            if (hasHeader) {
                if (reader.readLine() == null) {
                    return new CachedDataset(new double[0], new double[0], 0, lastModified);
                }
                lineNumber++;
            }
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                String[] parts = line.split(",", -1);
                if (parts.length != features + 1) {
                    throw new IllegalArgumentException("Invalid column count at line " + lineNumber);
                }
                if (rows == capacity) {
                    capacity *= 2;
                    featureData = Arrays.copyOf(featureData, capacity * features);
                    labelData = Arrays.copyOf(labelData, capacity);
                }
                int base = rows * features;
                double y;
                if ("first".equalsIgnoreCase(csvLabel)) {
                    y = Double.parseDouble(parts[0].trim());
                    for (int i = 0; i < features; i++) {
                        double xi = Double.parseDouble(parts[i + 1].trim());
                        if (normalize != 1.0) {
                            xi /= normalize;
                        }
                        featureData[base + i] = xi;
                    }
                } else {
                    for (int i = 0; i < features; i++) {
                        double xi = Double.parseDouble(parts[i].trim());
                        if (normalize != 1.0) {
                            xi /= normalize;
                        }
                        featureData[base + i] = xi;
                    }
                    y = Double.parseDouble(parts[features].trim());
                }
                labelData[rows] = y;
                rows++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read dataRef: " + path, e);
        }
        double[] finalFeatures = Arrays.copyOf(featureData, rows * features);
        double[] finalLabels = Arrays.copyOf(labelData, rows);
        return new CachedDataset(finalFeatures, finalLabels, rows, lastModified);
    }

    /**
     * Builds cache Key.
     */
    private static String buildCacheKey(Path path,
                                        int features,
                                        boolean hasHeader,
                                        String csvLabel,
                                        double normalize) {
        String label = (csvLabel == null || csvLabel.trim().isEmpty()) ? "last" : csvLabel.trim().toLowerCase();
        return path.toAbsolutePath() + "|" + features + "|" + hasHeader + "|" + label + "|" + normalize;
    }

    /**
     * Returns last Modified.
     */
    private static long getLastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * In-memory cache of features/labels for a CSV dataRef.
     */
    private static final class CachedDataset {
        private final double[] features;
        private final double[] labels;
        private final int rows;
        private final long lastModified;

        /**
         * Creates a CachedDataset.
         */
        private CachedDataset(double[] features, double[] labels, int rows, long lastModified) {
            this.features = features;
            this.labels = labels;
            this.rows = rows;
            this.lastModified = lastModified;
        }
    }

    /**
     * Performs resolve Data Ref.
     */
    private static Path resolveDataRef(String dataRef) {
        String ref = dataRef.trim();
        if (ref.startsWith("file:")) {
            ref = ref.substring("file:".length());
        }
        Path path = Paths.get(ref).normalize();
        if (Files.exists(path)) {
            return path;
        }
        Path fallback = Paths.get("src/de/luh/vss/chat/dtq/worker").resolve(path).normalize();
        if (Files.exists(fallback)) {
            return fallback;
        }
        Path fallbackData = Paths.get("src/de/luh/vss/chat/dtq/worker/data")
                .resolve(path.getFileName() != null ? path.getFileName().toString() : path.toString())
                .normalize();
        if (Files.exists(fallbackData)) {
            return fallbackData;
        }
        System.err.println("DataRef not found: " + dataRef + " (resolved to " + path.toString() + ")");
        throw new IllegalArgumentException("DATAREF_NOT_FOUND:" + dataRef);
    }

    /**
     * Performs map Label For Task.
     */
    private static double mapLabelForTask(String task, double label) {
        int target = parseBinaryTarget(task);
        if (target < 0) {
            return label;
        }
        int y = (int) Math.round(label);
        return y == target ? 1.0 : 0.0;
    }

    /**
     * Parses binary Target.
     */
    private static int parseBinaryTarget(String task) {
        if (task == null) {
            return -1;
        }
        String normalized = task.trim().toLowerCase();
        if (!normalized.startsWith("binary_is_")) {
            return -1;
        }
        String token = normalized.substring("binary_is_".length());
        if (token.length() == 1) {
            char ch = token.charAt(0);
            if (ch >= '0' && ch <= '9') {
                return ch - '0';
            }
        }
        switch (token) {
            case "zero":
                return 0;
            case "one":
                return 1;
            case "two":
                return 2;
            case "three":
                return 3;
            case "four":
                return 4;
            case "five":
                return 5;
            case "six":
                return 6;
            case "seven":
                return 7;
            case "eight":
                return 8;
            case "nine":
                return 9;
            default:
                return -1;
        }
    }

    /**
     * Performs sigmoid.
     */
    private static double sigmoid(double x) {
        if (x >= 0.0) {
            double z = Math.exp(-x);
            return 1.0 / (1.0 + z);
        }
        double z = Math.exp(x);
        return z / (1.0 + z);
    }
}
