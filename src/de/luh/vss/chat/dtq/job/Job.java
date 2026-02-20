package de.luh.vss.chat.dtq.job;

import de.luh.vss.chat.common.Message;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Represents job.
 */
public class Job {

    private final String jobId;
    private final int producerId;
    private JobStatus status;
    private final Message.JobType jobType;
    private String payload;
    private BlockingQueue<String> resultList = new LinkedBlockingQueue<>();

    // Full constructor
    public Job(
            String jobId,
            int producerId,
            JobStatus status,
            Message.JobType jobType,
            String payload
    ) {
        this.jobId = jobId;
        this.producerId = producerId;
        this.status = status;
        this.jobType = jobType;
        this.payload = payload;
    }

    // Convenience constructor: default status = SUBMITTED
    public Job(String jobId, int producerId, Message.JobType jobType, String payload) {
        this(jobId, producerId, JobStatus.SUBMITTED, jobType, payload);
    }

    //  getters 
    public String getJobId() { return jobId; }
    /**
     * Returns producer Id.
     */
    public int getProducerId() { return producerId; }
    /**
     * Returns status.
     */
    public JobStatus getStatus() { return status; }
    /**
     * Returns job Type.
     */
    public Message.JobType getJobType() { return jobType; }
    /**
     * Returns payload.
     */
    public String getPayload() { return payload; }

    //  setters 
    public void setStatus(JobStatus status) {
        if (status != null) {
            this.status = status;
        }
    }

    /**
     * Sets payload.
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }
    /**
     * Sets result.
     */
    public void setResult(String result){
        try {
            resultList.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
