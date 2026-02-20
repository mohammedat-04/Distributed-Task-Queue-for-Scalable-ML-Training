package de.luh.vss.chat.dtq.master;

import de.luh.vss.chat.common.Message;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Sends outbound messages from the master to workers and producers.
 */
public class MasterSendMessage implements Runnable {
    private final BlockingQueue<Message> toSendMessages = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<Integer, WorkerInfo> workers ;
    private final ConcurrentHashMap<Integer, ProducerInfo> producers ;  
    /**
     * Creates a MasterSendMessage.
     */
    public MasterSendMessage(ConcurrentHashMap<Integer, WorkerInfo> workers,ConcurrentHashMap<Integer, ProducerInfo> producers){
        this.workers = workers;
        this.producers = producers;
    }
    
   
    /**
     * Returns tosend Messages.
     */
    public BlockingQueue<Message> getTosendMessages(){
        return toSendMessages;
    }


    
    /**
     * Drains the outbound queue and writes messages to their targets.
     */
    @Override
    public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Message message = toSendMessages.take();
                    if(message instanceof Message.ChatMessagePayload chatMsg){
                        System.out.println("Sending chat message: " + chatMsg.getMessage()+" from "+chatMsg.getrole()+" with the ID:"+ chatMsg.getId()+"\n");
                        for (WorkerInfo worker : workers.values()) {
                            try {
                                synchronized (worker.getOutputStream()) {
                                    chatMsg.toStream(worker.getOutputStream());
                                }
                            } catch (IOException e) {
                                System.err.println("Failed to send chat message to worker " + worker.getWorkerId());
                            }
                        }
                        for (ProducerInfo producer : producers.values()) {
                            if (producer.getProducerId() == chatMsg.getId()) {
                                continue;
                            }
                            try {
                                synchronized (producer.getOutputStream()) {
                                    chatMsg.toStream(producer.getOutputStream());
                                }
                            } catch (IOException e) {
                                System.err.println("Failed to send chat message to producer " + producer.getProducerId());
                            }
                        }
                    }
                    if (message instanceof Message.MasterJobResult jobResult) {
                        ProducerInfo producer = producers.get(jobResult.getProducerId());
                        if (producer == null) {
                            System.err.println("Unknown producer for job result: " + jobResult.getProducerId());
                        } else {
                            synchronized (producer.getOutputStream()) {
                                jobResult.toStream(producer.getOutputStream());
                                producer.getOutputStream().flush();
                            }
                        }
                    }
                    if (message instanceof Message.EpochMetrics metrics) {
                        ProducerInfo producer = producers.get(metrics.getProducerId());
                        if (producer != null) {
                            synchronized (producer.getOutputStream()) {
                                metrics.toStream(producer.getOutputStream());
                                producer.getOutputStream().flush();
                            }
                        }
                    }
                    if(message instanceof Message.MasterAssignTask taskMsg){
                        WorkerInfo worker = workers.get(taskMsg.getWorkerId());
                        synchronized (worker.getOutputStream()) {
                            taskMsg.toStream(worker.getOutputStream());
                            worker.getOutputStream().flush();
                        }
                    }
                    if (message instanceof Message.MasterCancelTask cancel) {
                        for (WorkerInfo worker : workers.values()) {
                            try {
                                synchronized (worker.getOutputStream()) {
                                    cancel.toStream(worker.getOutputStream());
                                    worker.getOutputStream().flush();
                                }
                            } catch (IOException e) {
                                System.err.println("Failed to send cancel to worker " + worker.getWorkerId());
                            }
                        }
                    }
                } catch (InterruptedException | IOException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        
    }
    
}
