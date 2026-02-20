package de.luh.vss.chat.dtq.worker;

/**
 * Immutable gradient result for a worker task.
 */
public final class TaskResult {
    private final String jobId;
    private final int epoch;
    private final int batchId;
    private final int count;
    private final double lossSum;
    private final double[] gradSum;
    private final int attempt;

    /**
     * Creates a TaskResult.
     */
    public TaskResult(String jobId,
                      int epoch,
                      int batchId,
                      int count,
                      double lossSum,
                      double[] gradSum,
                      int attempt) {
        this.jobId = jobId;
        this.epoch = epoch;
        this.batchId = batchId;
        this.count = count;
        this.lossSum = lossSum;
        this.gradSum = gradSum;
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
     * Returns count.
     */
    public int getCount() {
        return count;
    }

    /**
     * Returns loss Sum.
     */
    public double getLossSum() {
        return lossSum;
    }

    /**
     * Returns grad Sum.
     */
    public double[] getGradSum() {
        return gradSum;
    }

    /**
     * Returns attempt.
     */
    public int getAttempt() {
        return attempt;
    }
}
