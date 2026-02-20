package de.luh.vss.chat.dtq.job;

import de.luh.vss.chat.common.Message;

/**
 * Represents sum Job.
 */
public class SumJob extends Job  {
    private String para; 
    
    /**
     * Creates a SumJob.
     */
    public SumJob(String jobId,int producerId,Message.JobType job,String payload){
        super(jobId, producerId, job, payload);
    }
    
}
