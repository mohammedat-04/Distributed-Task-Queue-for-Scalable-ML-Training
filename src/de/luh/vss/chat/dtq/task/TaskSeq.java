package de.luh.vss.chat.dtq.task;

import de.luh.vss.chat.common.Message;

/**
 * Represents task Seq.
 */
public class TaskSeq {

    private String jobId;
    private String taskId;
    private int workerId;
    private Message.JobType job;
    private TaskState state;
    private String payload;
    private String result;
    private long leaseUntil;
    private int attempts;
    private long nextEligibleAt;

    /**
     * Creates a TaskSeq.
     */
    public TaskSeq(
            String jobId,
            String taskId,
            int workerId,
            Message.JobType job,
            TaskState state,
            String payload,
            String result
    ) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.workerId = workerId;
        this.job = job;

        this.state = (state != null) ? state : TaskState.PENDING;
        this.payload = (payload != null) ? payload : "";
        this.result = (result != null) ? result : "";
        this.leaseUntil = 0L;
        this.attempts = 0;
        this.nextEligibleAt = 0L;
    }

    // --- getters ---
    public String getJobId() { return jobId; }
    /**
     * Returns task Id.
     */
    public String getTaskId() { return taskId; }
    /**
     * Returns worker Id.
     */
    public int getWorkerId() { return workerId; }
    /**
     * Returns job.
     */
    public Message.JobType getJob() { return job; }
    /**
     * Returns state.
     */
    public TaskState getState() { return state; }
    /**
     * Returns payload.
     */
    public String getPayload() { return payload; }
    /**
     * Returns result.
     */
    public String getResult() { return result; }
    /**
     * Returns lease Until.
     */
    public long getLeaseUntil() { return leaseUntil; }
    /**
     * Returns attempts.
     */
    public int getAttempts() { return attempts; }
    /**
     * Returns next Eligible At.
     */
    public long getNextEligibleAt() { return nextEligibleAt; }

    // --- setters (now mutation is allowed) ---
    public void setWorkerId(int workerId) {
        this.workerId = workerId;
    }

    /**
     * Sets state.
     */
    public void setState(TaskState state) {
        if (state != null) {
            this.state = state;
        }
    }

    /**
     * Sets payload.
     */
    public void setPayload(String payload) {
        this.payload = (payload != null) ? payload : "";
    }

    /**
     * Sets result.
     */
    public void setResult(String result) {
        this.result = (result != null) ? result : "";
    }

    /**
     * Sets lease Until.
     */
    public void setLeaseUntil(long leaseUntil) {
        this.leaseUntil = leaseUntil;
    }

    /**
     * Sets next Eligible At.
     */
    public void setNextEligibleAt(long nextEligibleAt) {
        this.nextEligibleAt = nextEligibleAt;
    }

    /**
     * Performs increment Attempts.
     */
    public void incrementAttempts() {
        attempts++;
    }
}
