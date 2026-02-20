package de.luh.vss.chat.dtq.job;

import de.luh.vss.chat.common.Message;
import java.util.Arrays;

/**
 * Represents train Gd Batch Job.
 */
public class TrainGdBatchJob extends Job {

    private String model = "linreg";
    private String task = "";
    private int samples;
    private int features;
    private int epochs;
    private int batchSize;
    private double lr;
    private int seed;
    private String dataRef = "";
    private boolean hasHeader;
    private String csvLabel = "last";
    private double normalize = 1.0;
    private String init = "zeros";
    private int epoch;
    private double[] weights;
    private double[] gradAcc;
    private double lossAcc;
    private int countAcc;
    private double totalLossAcc;
    private int totalCountAcc;
    private double epochLossAcc;
    private int epochCountAcc;
    private double bestEpochLoss = Double.POSITIVE_INFINITY;
    private double[] bestWeights;
    private int earlyStopPatience = 3;
    private double earlyStopMinDelta = 1e-6;
    private int epochsWithoutImprovement;
    private boolean earlyStop;
    private int batchesTotal;
    private int bacthesDone;
    private String rawWeights;
    private boolean async;
    private double[] lastEpochWeights;

    /**
     * Snapshot returned after an epoch completes.
     */
    public static final class EpochUpdate {
        public final int epoch;
        public final double avgLoss;
        public final int count;
        public final double[] weights;

        public EpochUpdate(int epoch, double avgLoss, int count, double[] weights) {
            this.epoch = epoch;
            this.avgLoss = avgLoss;
            this.count = count;
            this.weights = weights;
        }
    }

    /**
     * Creates a TrainGdBatchJob.
     */
    public TrainGdBatchJob(String jobId, int producerId, String str) {
        super(jobId, producerId, Message.JobType.GRADIEN_JOB, str);
        convertStrToparmenter(str);
    }

    /**
     * Performs convert Str Toparmenter.
     */
    private void convertStrToparmenter(String str) {
        if (str == null || str.trim().isEmpty()) {
            return;
        }
        String[] parts = str.split("\\|");
        for (String part : parts) {
            String token = part.trim();
            if (token.isEmpty()) {
                continue;
            }
            String[] kv = token.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            String key = kv[0].trim();
            String value = kv[1].trim();
            switch (key) {
                case "model":
                    model = value;
                    break;
                case "task":
                    task = value;
                    break;
                case "samples":
                    samples = parseIntSafe(value, samples);
                    break;
                case "features":
                    features = parseIntSafe(value, features);
                    break;
                case "epochs":
                    epochs = parseIntSafe(value, epochs);
                    break;
                case "batchSize":
                    batchSize = parseIntSafe(value, batchSize);
                    break;
                case "lr":
                    lr = parseDoubleSafe(value, lr);
                    break;
                case "seed":
                    seed = parseIntSafe(value, seed);
                    break;
                case "dataRef":
                    dataRef = value;
                    break;
                case "hasHeader":
                    hasHeader = Boolean.parseBoolean(value);
                    break;
                case "csvLabel":
                    csvLabel = value;
                    break;
                case "normalize":
                    normalize = parseDoubleSafe(value, normalize);
                    break;
                case "init":
                    init = value;
                    break;
                case "patience":
                    earlyStopPatience = parseIntSafe(value, earlyStopPatience);
                    break;
                case "minDelta":
                    earlyStopMinDelta = parseDoubleSafe(value, earlyStopMinDelta);
                    break;
                case "mode":
                    async = "async".equalsIgnoreCase(value);
                    break;
                case "async":
                    async = Boolean.parseBoolean(value);
                    break;
                case "sync":
                    async = !Boolean.parseBoolean(value);
                    break;
                case "w":
                    rawWeights = value;
                    break;
                default:
                    break;
            }
        }
        if (earlyStopPatience < 0) {
            earlyStopPatience = 0;
        }
        if (samples > 0 && batchSize > 0) {
            batchesTotal = (samples + batchSize - 1) / batchSize;
        }
        if (features > 0) {
            gradAcc = new double[features];
            if (rawWeights != null && !rawWeights.isEmpty()) {
                double[] parsed = parseWeights(rawWeights, features);
                if (parsed != null) {
                    weights = parsed;
                }
            }
            if (weights == null || weights.length != features) {
                weights = new double[features];
            }
        }
    }

    /**
     * Parses int Safe.
     */
    private static int parseIntSafe(String value, int fallback) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses double Safe.
     */
    private static double parseDoubleSafe(String value, double fallback) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * Parses weights.
     */
    private static double[] parseWeights(String raw, int features) {
        String[] parts = raw.split(",");
        if (parts.length < features) {
            System.err.println("Weight vector too short: " + parts.length + " < " + features);
            return null;
        }
        double[] parsed = new double[features];
        for (int i = 0; i < features; i++) {
            try {
                parsed[i] = Double.parseDouble(parts[i].trim());
            } catch (NumberFormatException e) {
                System.err.println("Invalid weight at index " + i);
                return null;
            }
        }
        return parsed;
    }

    /**
     * Returns the total number of samples in the training set.
     */
    public int getSamples() {
        return samples;
    }

    /**
     * Returns the number of features per sample.
     */
    public int getFeatures() {
        return features;
    }

    /**
     * Returns the number of training epochs.
     */
    public int getEpochs() {
        return epochs;
    }

    /**
     * Returns the mini-batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Returns the model identifier ( linreg or logreg).
     */
    public String getModel() {
        return model;
    }

    /**
     * Returns the training task identifier ( binary_is_zero).
     */
    public String getTask() {
        return task;
    }

    /**
     * Returns the data reference ( file:./data/train.csv).
     */
    public String getDataRef() {
        return dataRef;
    }

    /**
     * Returns the current weight vector, if initialized.
     */
    public double[] getWeights() {
        return weights;
    }

    /**
     * Returns has Header.
     */
    public boolean getHasHeader() {
        return hasHeader;
    }

    /**
     * Returns csv Label.
     */
    public String getCsvLabel() {
        return csvLabel;
    }

    /**
     * Returns normalize.
     */
    public double getNormalize() {
        return normalize;
    }

    /**
     * Returns whether async.
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * Performs accumulate Gradient.
     */
    public void accumulateGradient(int count, double lossSum, double[] gradSum) {
        if (count <= 0 || gradSum == null) {
            return;
        }
        if (weights == null || weights.length != features) {
            weights = new double[features];
        }
        if (gradAcc == null || gradAcc.length != features) {
            gradAcc = new double[features];
        }
        int limit = Math.min(gradAcc.length, gradSum.length);
        for (int i = 0; i < limit; i++) {
            gradAcc[i] += gradSum[i];
        }
        lossAcc += lossSum;
        countAcc += count;
        totalLossAcc += lossSum;
        totalCountAcc += count;
        epochLossAcc += lossSum;
        epochCountAcc += count;
    }

    /**
     * Applies gradient.
     */
    public void applyGradient(int count, double lossSum, double[] gradSum) {
        if (count <= 0 || gradSum == null) {
            return;
        }
        if (weights == null || weights.length != features) {
            weights = new double[features];
        }
        double step = lr / count;
        int limit = Math.min(weights.length, gradSum.length);
        for (int i = 0; i < limit; i++) {
            weights[i] -= step * gradSum[i];
        }
        totalLossAcc += lossSum;
        totalCountAcc += count;
    }

    /**
     * Applies accumulated gradient and returns an epoch snapshot when an epoch ends.
     */
    public EpochUpdate applyAccumulatedGradientWithMetrics() {
        if (countAcc <= 0) {
            return null;
        }
        double step = lr / countAcc;
        int limit = Math.min(weights.length, gradAcc.length);
        for (int i = 0; i < limit; i++) {
            weights[i] -= step * gradAcc[i];
        }
        Arrays.fill(gradAcc, 0.0);
        lossAcc = 0.0;
        countAcc = 0;
        bacthesDone++;

        if (batchesTotal > 0 && bacthesDone % batchesTotal == 0) {
            epoch++;
            EpochUpdate update = null;
            if (epochCountAcc > 0) {
                double avgEpochLoss = epochLossAcc / epochCountAcc;
                lastEpochWeights = Arrays.copyOf(weights, weights.length);
                update = new EpochUpdate(epoch, avgEpochLoss, epochCountAcc, lastEpochWeights);
                if (avgEpochLoss + earlyStopMinDelta < bestEpochLoss) {
                    bestEpochLoss = avgEpochLoss;
                    bestWeights = Arrays.copyOf(weights, weights.length);
                    epochsWithoutImprovement = 0;
                } else if (earlyStopPatience > 0) {
                    epochsWithoutImprovement++;
                    if (epochsWithoutImprovement >= earlyStopPatience) {
                        earlyStop = true;
                        if (bestWeights != null) {
                            weights = Arrays.copyOf(bestWeights, bestWeights.length);
                        }
                    }
                }
            }
            epochLossAcc = 0.0;
            epochCountAcc = 0;
            return update;
        }
        return null;
    }

    /**
     * Returns whether more Batches.
     */
    public boolean hasMoreBatches() {
        if (earlyStop) {
            return false;
        }
        return batchesTotal > 0 && epochs > 0 && bacthesDone < batchesTotal * epochs;
    }

    /**
     * Returns current Batch Start.
     */
    public int getCurrentBatchStart() {
        if (batchesTotal <= 0) {
            return 0;
        }
        int batchIndex = bacthesDone % batchesTotal;
        return batchIndex * batchSize;
    }

    /**
     * Returns current Batch Count.
     */
    public int getCurrentBatchCount() {
        int start = getCurrentBatchStart();
        int remaining = samples - start;
        if (remaining <= 0) {
            return 0;
        }
        return Math.min(batchSize, remaining);
    }

    /**
     * Returns current Epoch.
     */
    public int getCurrentEpoch() {
        if (batchesTotal <= 0) {
            return 0;
        }
        return bacthesDone / batchesTotal;
    }

    /**
     * Returns current Batch Id.
     */
    public int getCurrentBatchId() {
        if (batchesTotal <= 0) {
            return 0;
        }
        return bacthesDone % batchesTotal;
    }

    /**
     * Builds result Payload.
     */
    public String buildResultPayload() {
        StringBuilder sb = new StringBuilder();
        sb.append("weights=");
        if (weights != null) {
            for (int i = 0; i < weights.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(weights[i]);
            }
        }
        if (totalCountAcc > 0) {
            sb.append("|loss=").append(totalLossAcc);
        }
        sb.append("|count=").append(totalCountAcc);
        return sb.toString();
    }

    /**
     * Returns current weights as CSV string.
     */
    public String weightsAsCsv() {
        if (weights == null || weights.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < weights.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(weights[i]);
        }
        return sb.toString();
    }
}
