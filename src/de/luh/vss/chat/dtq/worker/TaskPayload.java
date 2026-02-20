package de.luh.vss.chat.dtq.worker;

/**
 * Immutable description of a worker task for gradient computation.
 */
public final class TaskPayload {
    private final String jobId;
    private final int epoch;
    private final int batchId;
    private final int start;
    private final int count;
    private final int features;
    private final String model;
    private final String task;
    private final String dataRef;
    private final double[] w;
    private final boolean hasHeader;
    private final String csvLabel;
    private final double normalize;
    private final int attempt;

    /**
     * Creates a TaskPayload.
     */
    public TaskPayload(String jobId,
                       int epoch,
                       int batchId,
                       int start,
                       int count,
                       int features,
                       String model,
                       String task,
                       String dataRef,
                       double[] w,
                       boolean hasHeader,
                       String csvLabel,
                       double normalize,
                       int attempt) {
        this.jobId = jobId;
        this.epoch = epoch;
        this.batchId = batchId;
        this.start = start;
        this.count = count;
        this.features = features;
        this.model = model;
        this.task = task;
        this.dataRef = dataRef;
        this.w = w;
        this.hasHeader = hasHeader;
        this.csvLabel = csvLabel;
        this.normalize = normalize;
        this.attempt = attempt;
    }

    /**
     * Returns job Id.
     */
    public String getJobId() {
        return jobId;
    }

    /**
     * Returns epoch.
     */
    public int getEpoch() {
        return epoch;
    }

    /**
     * Returns batch Id.
     */
    public int getBatchId() {
        return batchId;
    }

    /**
     * Returns start.
     */
    public int getStart() {
        return start;
    }

    /**
     * Returns count.
     */
    public int getCount() {
        return count;
    }

    /**
     * Returns features.
     */
    public int getFeatures() {
        return features;
    }

    /**
     * Returns model.
     */
    public String getModel() {
        return model;
    }

    /**
     * Returns task.
     */
    public String getTask() {
        return task;
    }

    /**
     * Returns data Ref.
     */
    public String getDataRef() {
        return dataRef;
    }

    /**
     * Returns w.
     */
    public double[] getW() {
        return w;
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
     * Returns attempt.
     */
    public int getAttempt() {
        return attempt;
    }
}
