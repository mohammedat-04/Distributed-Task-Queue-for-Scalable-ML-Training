package de.luh.vss.chat.dtq.master;

import de.luh.vss.chat.common.Message;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Bootstraps the master server, lease manager, sender, and message handler.
 */
public class Master {

    private static  final int port = 44444;
    
    
    /**
     * Application entry point.
     */
    public static void main(String[] args) throws IOException {
        ConcurrentHashMap<Integer, WorkerInfo> workers ;
        ConcurrentHashMap<Integer, ProducerInfo> producers ;
        String myIp = java.net.InetAddress.getLocalHost().getHostAddress();
        System.out.println("My IP: " + myIp);
        MasterServer masterServer = new MasterServer(port);
        Thread masterServerThread = new Thread(masterServer); 
        masterServerThread.start();
        // Start Lease Manager
        MasterLease leaseManager = new MasterLease(masterServer.getWorkers(),masterServer.getProducers());
        Thread leaseManagerThread = new Thread(leaseManager);
        leaseManagerThread.start();

        MasterSendMessage masterSendMessage = new MasterSendMessage(masterServer.getWorkers(),masterServer.getProducers());
        Thread masterSendMessageThread = new Thread(masterSendMessage);
        masterSendMessageThread.start();
        BlockingQueue<Message> toSendMessages = masterSendMessage.getTosendMessages();
        // Start Message Handler
        MasterMessageHandler messageHandler = new MasterMessageHandler(masterServer.getMessageQueue(),toSendMessages,masterServer.getWorkers(),masterServer.getProducers());
        
        Thread messageHandlerThread = new Thread(messageHandler);
        messageHandler.loadPersistedJobs();
        messageHandlerThread.start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter 'exit' to shut down the master server:");
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Shutting down master server...");
                messageHandler.persistCompletedJobs();
                masterServerThread.interrupt();
                leaseManagerThread.interrupt();
                messageHandlerThread.interrupt();
                break;
            }
            if (input.equalsIgnoreCase("jobs")) {
                var snapshot = messageHandler.snapshotJobStates();
                var jobProducers = messageHandler.snapshotJobProducers();
                if (snapshot.isEmpty()) {
                    System.out.println("No jobs submitted.");
                } else {
                    for (var entry : snapshot.entrySet()) {
                        Integer producerId = jobProducers.get(entry.getKey());
                        String producerInfo = producerId != null ? " (producer=" + producerId + ")" : "";
                        System.out.println(entry.getKey() + " -> " + entry.getValue() + producerInfo);
                    }
                }
                continue;
            }
            if (input.equalsIgnoreCase("info")) {
                String ip = java.net.InetAddress.getLocalHost().getHostAddress();
                System.out.println("Master IP: " + ip + " | Port: " + port);
                continue;
            }
            if (input.toLowerCase().startsWith("workers")) {
                boolean verbose = input.toLowerCase().contains("verbose");
                var workerMap = masterServer.getWorkers();
                var jobProgress = messageHandler.snapshotJobProgress();
                int totalTasks = 0;
                int remainingTasks = 0;
                for (var jp : jobProgress) {
                    totalTasks += jp.totalTasks;
                    remainingTasks += jp.remainingTasks;
                }
                System.out.println("Tasks: total=" + totalTasks + " remaining=" + remainingTasks);
                if (!jobProgress.isEmpty()) {
                    System.out.println("Jobs:");
                    for (var jp : jobProgress) {
                        String batchInfo = "";
                        if (jp.batchesPerEpoch > 0) {
                            batchInfo = " | epoch=" + jp.epoch + " batch=" + jp.batchId + "/" + jp.batchesPerEpoch;
                        }
                        System.out.println(jp.jobId + " | status=" + jp.status
                                + " | tasks=" + jp.totalTasks
                                + " remaining=" + jp.remainingTasks
                                + batchInfo);
                    }
                }
                if (workerMap.isEmpty()) {
                    System.out.println("No active workers.");
                    continue;
                }
                long now = System.currentTimeMillis();
                System.out.println("Worker performance:");
                for (var entry : workerMap.entrySet()) {
                    int workerId = entry.getKey();
                    WorkerInfo info = entry.getValue();
                    var perf = messageHandler.getWorkerPerfSnapshot(workerId);
                    long leaseAge = now - info.lastLease;
                    System.out.println("Worker " + workerId
                            + " | assigned=" + perf.assigned
                            + " completed=" + perf.completed
                            + " failed=" + perf.failed
                            + " inFlight=" + perf.inFlight
                            + " avgMs=" + perf.avgMs
                            + " leaseAgeMs=" + leaseAge);
                }
                if (verbose) {
                    var inflight = messageHandler.snapshotInFlight(now);
                    if (inflight.isEmpty()) {
                        System.out.println("No in-flight tasks.");
                    } else {
                        System.out.println("In-flight tasks:");
                        for (var entry : inflight.entrySet()) {
                            int workerId = entry.getKey();
                            for (var snap : entry.getValue()) {
                                System.out.println("Worker " + workerId
                                        + " | task=" + snap.taskId
                                        + " ageMs=" + snap.ageMs);
                            }
                        }
                    }
                }
                continue;
            }
            if(input.startsWith("list")){
                System.out.println("Active Workers:");
                if(masterServer.getWorkers().isEmpty() && masterServer.getProducers().isEmpty()){
                    System.out.println("No active workers.");
                }
                else{
                    for(var entry : masterServer.getWorkers().entrySet()){
                        System.out.println("Worker ID: " + entry.getKey() + ", Last Lease: " + entry.getValue().lastLease);
                    }
                    for(var entry : masterServer.getProducers().entrySet()){
                        System.out.println("Producer ID: " + entry.getKey() + ", Last Lease: " + entry.getValue().lastLease);
                    }
                }
              
                
            }
        }
        
      
    }
}
