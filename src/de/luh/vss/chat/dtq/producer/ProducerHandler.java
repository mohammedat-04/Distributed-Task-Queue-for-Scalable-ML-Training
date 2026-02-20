package de.luh.vss.chat.dtq.producer;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.dtq.metrics.ExperimentLogger;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.imageio.ImageIO;

/**
 * Processes master messages and materializes job outputs (loss, accuracy, weights).
 */
public class ProducerHandler implements Runnable {
    private final BlockingQueue<Message> receivedMessage;
    private final BlockingQueue<String> sendMessage = new LinkedBlockingQueue<>();
    private final Map<String, String> jobStates;
    private final Map<String, String> jobResults;
    private final Map<String, String> jobPayloads;
    private final Map<String, Long> jobSubmitTimes;
    private final Map<String, Long> jobDurations;
    private final Runnable stateSaver;
    /**
     * Creates a ProducerHandler.
     */
    public ProducerHandler(BlockingQueue<Message> receivedMessage, Map<String, String> jobStates,
                           Map<String, String> jobResults, Map<String, String> jobPayloads,
                           Map<String, Long> jobSubmitTimes, Map<String, Long> jobDurations,
                           Runnable stateSaver){
        this.receivedMessage = receivedMessage;
        this.jobStates = jobStates;
        this.jobResults = jobResults;
        this.jobPayloads = jobPayloads;
        this.jobSubmitTimes = jobSubmitTimes;
        this.jobDurations = jobDurations;
        this.stateSaver = stateSaver;
        
    }
    
    /**
     * Returns send Message Queue.
     */
    public BlockingQueue<String> getSendMessageQueue(){return sendMessage;}
    /**
     * Handles message.
     */
    private void handleMessage(Message message) throws InterruptedException{
        if(message instanceof Message.ServiceErrorResponse errorResponse){
            System.err.println("Received error from server: " + errorResponse.getErrorMessage());
            System.exit(1);
        } else if (message instanceof Message.ChatMessagePayload chatMessage) {
            System.out.println("\u001B[36m[CHAT]\u001B[0m " +
                    chatMessage.getrole() + " " + chatMessage.getId() + ": " + chatMessage.getMessage());
            String text = chatMessage.getMessage();
            if (text.startsWith("JOB_STATE ")) {
                String[] parts = text.split("\\s+", 3);
                if (parts.length == 3) {
                    jobStates.put(parts[1], parts[2]);
                    if (stateSaver != null) {
                        stateSaver.run();
                    }
                }
            }
        } else if (message instanceof Message.EpochMetrics epochMetrics) {
            handleEpochMetrics(epochMetrics);
        } else if (message instanceof Message.MasterJobResult jobResult) {
            jobStates.put(jobResult.getJobId(), "COMPLETED");
            jobResults.put(jobResult.getJobId(), jobResult.getResult());
            Long start = jobSubmitTimes.get(jobResult.getJobId());
            Long duration = null;
            if (start != null) {
                duration = System.currentTimeMillis() - start;
                jobDurations.put(jobResult.getJobId(), duration);
            }
            Map<String, String> resFields = parsePayloadFields(jobResult.getResult());
            String displayResult = compactPayloadField(jobResult.getResult(), "weights");
            String line = "\u001B[32m[JOB RESULT]\u001B[0m " +
                    jobResult.getJobId() + ": " + displayResult;
            if (duration != null) {
                line += " | timeMs: " + duration;
            }
            System.out.println(line);
            handleTrainJobResult(jobResult.getJobId(), jobResult.getResult());
            if (duration != null) {
                int workers = parseIntSafe(resFields.get("workers"), 0);
                String mode = resFields.getOrDefault("mode", "sync");
                double throughput = parseDoubleSafe(resFields.get("throughput"), -1.0);
                long wallMs = parseLongSafe(resFields.get("wallMs"), duration);
                ExperimentLogger.logJobSummary(jobResult.getJobId(), wallMs, throughput, workers, mode);
                if (workers > 0) {
                    ExperimentLogger.logSpeedupPoint(jobResult.getJobId(), workers, mode, wallMs);
                }
            }
            if (stateSaver != null) {
                stateSaver.run();
            }
        }
    }
    /**
     * Runs the task loop.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                handleMessage(receivedMessage.take());
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Processes per epoch metrics coming from the master and logs them.
     */
    private void handleEpochMetrics(Message.EpochMetrics epochMetrics) {
        String jobId = epochMetrics.getJobId();
        Map<String, String> payloadFields = parsePayloadFields(jobPayloads.get(jobId));
        double[] weights = parseWeights(epochMetrics.getWeightsCsv());
        double accuracy = Double.isNaN(epochMetrics.getAccuracy()) && weights.length > 0
                ? computeAccuracy(
                        payloadFields.getOrDefault("dataRef", payloadFields.get("testRef")),
                        parseIntSafe(payloadFields.get("features"), weights.length),
                        payloadFields.getOrDefault("model", "linreg"),
                        payloadFields.getOrDefault("task", ""),
                        weights,
                        Boolean.parseBoolean(payloadFields.getOrDefault("hasHeader", "false")),
                        payloadFields.getOrDefault("csvLabel", "last"),
                        parseDoubleSafe(payloadFields.get("normalize"), 1.0))
                : epochMetrics.getAccuracy();
        ExperimentLogger.logEpoch(
                jobId,
                epochMetrics.getEpoch(),
                epochMetrics.getLoss(),
                accuracy,
                epochMetrics.getDurationMs(),
                epochMetrics.getThroughput(),
                epochMetrics.getWorkers(),
                epochMetrics.getMode(),
                epochMetrics.getWeightsCsv());
        System.out.println("\u001B[35m[EPOCH]\u001B[0m " + jobId +
                " e=" + epochMetrics.getEpoch() +
                " loss=" + epochMetrics.getLoss() +
                " acc=" + String.format("%.4f", accuracy) +
                " durMs=" + epochMetrics.getDurationMs());
    }
    
    /**
     * Computes metrics and exports artifacts for a completed training job.
     */
    private void handleTrainJobResult(String jobId, String result) {
        if (result == null || result.isEmpty()) {
            return;
        }
        Map<String, String> resultFields = parsePayloadFields(result);
        String weightsRaw = resultFields.get("weights");
        if (weightsRaw == null || weightsRaw.isEmpty()) {
            return;
        }
        double[] weights = parseWeights(weightsRaw);
        double loss = parseDoubleSafe(resultFields.get("loss"), Double.NaN);
        int count = parseIntSafe(resultFields.get("count"), 0);
        if (!Double.isNaN(loss) && count > 0) {
            double avgLoss = loss / count;
            System.out.println("avgLoss = " + avgLoss);
        }

        Map<String, String> jobFields = parsePayloadFields(jobPayloads.get(jobId));
        String testRef = jobFields.get("testRef");
        if (testRef == null || testRef.isEmpty()) {
            testRef = jobFields.get("dataRef");
        }
        int features = parseIntSafe(jobFields.get("features"), weights.length);
        String model = jobFields.getOrDefault("model", "linreg");
        String task = jobFields.getOrDefault("task", "");
        boolean hasHeader = Boolean.parseBoolean(jobFields.getOrDefault("hasHeader", "false"));
        String csvLabel = jobFields.getOrDefault("csvLabel", "last");
        double normalize = parseDoubleSafe(jobFields.get("normalize"), 1.0);
        if (testRef != null && !testRef.isEmpty() && weights.length >= features) {
            double accuracy = computeAccuracy(testRef, features, model, task, weights,
                    hasHeader, csvLabel, normalize);
            if (accuracy >= 0.0) {
                System.out.println("accuracy = " + accuracy);
            }
        }

        String imagePath = exportWeightsImage(jobId, weights);
        if (imagePath != null) {
            System.out.println("weights image = " + imagePath);
        }
        String weightsPath = exportWeightsFile(jobId, weightsRaw);
        if (weightsPath != null) {
            System.out.println("weights file = " + weightsPath);
        }
    }

    /**
     * Performs compact Payload Field.
     */
    private String compactPayloadField(String payload, String key) {
        if (payload == null || payload.isEmpty()) {
            return payload;
        }
        String token = key + "=";
        int startKey = payload.indexOf(token);
        if (startKey < 0) {
            return payload;
        }
        int startValue = startKey + token.length();
        int endValue = payload.indexOf('|', startValue);
        if (endValue < 0) {
            endValue = payload.length();
        }
        String value = payload.substring(startValue, endValue).trim();
        if (value.length() <= 120) {
            return payload;
        }
        int count = 0;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) == ',') {
                count++;
            }
        }
        if (!value.isEmpty()) {
            count += 1;
        }
        String replacement = count + " params";
        return payload.substring(0, startValue) + replacement + payload.substring(endValue);
    }

    /**
     * Parses payload Fields.
     */
    private Map<String, String> parsePayloadFields(String payload) {
        Map<String, String> fields = new HashMap<>();
        if (payload == null || payload.isEmpty()) {
            return fields;
        }
        String[] parts = payload.split("\\|");
        for (String part : parts) {
            String token = part.trim();
            if (token.isEmpty()) {
                continue;
            }
            String[] kv = token.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            fields.put(kv[0].trim(), kv[1].trim());
        }
        return fields;
    }

    /**
     * Parses weights.
     */
    private double[] parseWeights(String raw) {
        String[] parts = raw.split(",");
        double[] weights = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String value = parts[i].trim();
            if (!value.isEmpty()) {
                weights[i] = Double.parseDouble(value);
            }
        }
        return weights;
    }

    /**
     * Parses int Safe.
     */
    private int parseIntSafe(String value, int fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses double Safe.
     */
    private double parseDoubleSafe(String value, double fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses long with fallback.
     */
    private long parseLongSafe(String value, long fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Computes accuracy.
     */
    private double computeAccuracy(String dataRef, int features, String model, String task,
                                   double[] weights, boolean hasHeader,
                                   String csvLabel, double normalize) {
        if (dataRef == null || dataRef.isEmpty()) {
            return -1.0;
        }
        if (features <= 0 || weights == null || weights.length < features) {
            return -1.0;
        }
        if (normalize <= 0.0) {
            normalize = 1.0;
        }
        Path path = resolveDataRef(dataRef);
        long total = 0;
        long correct = 0;
        double[] x = new double[features];
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            if (hasHeader) {
                if (reader.readLine() == null) {
                    return -1.0;
                }
            }
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length != features + 1) {
                    continue;
                }
                double y;
                if ("first".equalsIgnoreCase(csvLabel)) {
                    y = Double.parseDouble(parts[0].trim());
                    for (int i = 0; i < features; i++) {
                        double xi = Double.parseDouble(parts[i + 1].trim());
                        if (normalize != 1.0) {
                            xi /= normalize;
                        }
                        x[i] = xi;
                    }
                } else {
                    for (int i = 0; i < features; i++) {
                        double xi = Double.parseDouble(parts[i].trim());
                        if (normalize != 1.0) {
                            xi /= normalize;
                        }
                        x[i] = xi;
                    }
                    y = Double.parseDouble(parts[features].trim());
                }
                y = mapLabelForTask(task, y);
                double score = dot(weights, x, features);
                double pred = "logreg".equalsIgnoreCase(model) ? sigmoid(score) : score;
                int predicted = pred >= 0.5 ? 1 : 0;
                int actual = y >= 0.5 ? 1 : 0;
                if (predicted == actual) {
                    correct++;
                }
                total++;
            }
        } catch (IOException | NumberFormatException e) {
            System.err.println("Failed to compute accuracy: " + e.getMessage());
            return -1.0;
        }
        if (total == 0) {
            return -1.0;
        }
        return (double) correct / (double) total;
    }

    /**
     * Performs map Label For Task.
     */
    private double mapLabelForTask(String task, double label) {
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
    private int parseBinaryTarget(String task) {
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
     * Performs dot.
     */
    private double dot(double[] weights, double[] x, int features) {
        double sum = 0.0;
        for (int i = 0; i < features; i++) {
            sum += weights[i] * x[i];
        }
        return sum;
    }

    /**
     * Performs sigmoid.
     */
    private double sigmoid(double x) {
        if (x >= 0) {
            double z = Math.exp(-x);
            return 1.0 / (1.0 + z);
        }
        double z = Math.exp(x);
        return z / (1.0 + z);
    }

    /**
     * Performs resolve Data Ref.
     */
    private Path resolveDataRef(String dataRef) {
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
        return path;
    }

    /**
     * Performs export Weights Image.
     */
    private String exportWeightsImage(String jobId, double[] weights) {
        if (weights == null || weights.length != 28 * 28) {
            return null;
        }
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (double w : weights) {
            if (w < min) {
                min = w;
            }
            if (w > max) {
                max = w;
            }
        }
        double range = max - min;
        if (range <= 0.0) {
            range = 1.0;
        }
        BufferedImage image = new BufferedImage(28, 28, BufferedImage.TYPE_BYTE_GRAY);
        for (int i = 0; i < weights.length; i++) {
            int x = i % 28;
            int y = i / 28;
            int value = (int) Math.round((weights[i] - min) / range * 255.0);
            if (value < 0) {
                value = 0;
            } else if (value > 255) {
                value = 255;
            }
            image.getRaster().setSample(x, y, 0, value);
        }
        Path out = Paths.get("weights_" + jobId + ".png");
        try {
            ImageIO.write(image, "png", out.toFile());
            return out.toAbsolutePath().toString();
        } catch (IOException e) {
            System.err.println("Failed to write weights image: " + e.getMessage());
            return null;
        }
    }

    /**
     * Performs export Weights File.
     */
    private String exportWeightsFile(String jobId, String weightsRaw) {
        if (weightsRaw == null || weightsRaw.isEmpty()) {
            return null;
        }
        Path out = Paths.get("weights_" + jobId + ".csv");
        try {
            Files.writeString(out, weightsRaw, StandardCharsets.UTF_8);
            return out.toAbsolutePath().toString();
        } catch (IOException e) {
            System.err.println("Failed to write weights file: " + e.getMessage());
            return null;
        }
    }
}
