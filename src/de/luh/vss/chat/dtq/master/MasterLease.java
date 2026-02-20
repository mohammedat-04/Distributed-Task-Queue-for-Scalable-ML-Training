package de.luh.vss.chat.dtq.master;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Periodically checks worker/producer leases and removes expired entries.
 */
public class MasterLease implements Runnable {

    private ConcurrentHashMap<Integer, WorkerInfo> workers ;
    private ConcurrentHashMap<Integer, ProducerInfo> producers ;

    private final long leaseMs = 40_000; // 40 seconds lease duration

    /**
     * Creates a MasterLease.
     */
    public MasterLease( ConcurrentHashMap<Integer,WorkerInfo> workers, ConcurrentHashMap<Integer,ProducerInfo> producers){
        this.workers = workers;
        this.producers = producers;
       
    }

    /**
     * Scans for expired leases and closes sockets for stale peers.
     */
    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            long currentTime = System.currentTimeMillis();
            // Check and remove expired workers
            workers.entrySet().removeIf(entry -> {
                WorkerInfo worker = entry.getValue();
                if (System.currentTimeMillis() - worker.lastLease > leaseMs) { // lease expired
                    try {
                        worker.getWorkerSocket().close();
                        System.err.println("i will close the socket of the producer");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return true;
                }
                return false;
            });
      
            // Check and remove expired producers
            producers.entrySet().removeIf(entry -> {
                ProducerInfo producer = entry.getValue();
                if (System.currentTimeMillis() - producer.lastLease > leaseMs) { // lease expired
                    try {
                        producer.getProducerSocket().close();
                        System.err.println("i will close the socket of the producer");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return true;
                }
                return false;
            });
            try {
                 Thread.sleep(this.leaseMs /3);// Sleep for half the lease duration before next check
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            
        }
        
    }
    
}
