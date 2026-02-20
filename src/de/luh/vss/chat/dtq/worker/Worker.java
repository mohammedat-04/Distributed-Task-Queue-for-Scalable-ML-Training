package de.luh.vss.chat.dtq.worker;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.common.SystemRole;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Entry point for a DTQ worker process.
 */
public class Worker {
    /**
     * Starts a worker process that registers with the master, listens for tasks,
     * and sends task requests or chat messages based on user input.
     */
    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
        int id = Integer.parseInt(args[0]);
        int slots = Integer.parseInt(args[1]);
        long taskTimeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 0L;
        Connection connection = new Connection("10.172.119.178", 44444,id);
        WorkerServerConnection workerServerConnection = new WorkerServerConnection(connection);
        Thread serverConnectionThread = new Thread(workerServerConnection);
        serverConnectionThread.start();  
         Thread.sleep(500);
        workerServerConnection.awaitRegistration();
        System.out.println("Worker registered successfully");

        BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
        WokerReceiver workerReceiver = new WokerReceiver(connection, messageQueue);
        Thread receiverThread = new Thread(workerReceiver);
        receiverThread.start();

        WorkerHandler workerHandler = new WorkerHandler(messageQueue, slots, connection, taskTimeoutMs);
        Thread handlerThread = new Thread(workerHandler);
        handlerThread.start();
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter message to send (or 'exit' to quit):");
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Shutting down worker...");
                serverConnectionThread.interrupt();
                receiverThread.interrupt();
                break;
            }
            if (input.equalsIgnoreCase("stats")) {
                WorkerHandler.WorkerStats stats = workerHandler.snapshotStats();
                System.out.println("Worker stats: completed=" + stats.completed
                        + " failed=" + stats.failed
                        + (stats.lastTaskId != null ? " lastTask=" + stats.lastTaskId : "")
                        + (stats.lastError != null ? " lastError=" + stats.lastError : ""));
                continue;
            }
            if (input.equalsIgnoreCase("REQUEST_TASK")) {
                Message.WorkerRequestTask req = new Message.WorkerRequestTask(connection.getId(), slots);
                connection.writeMessage(req);
            } else {
                Message.ChatMessagePayload chat = new Message.ChatMessagePayload(SystemRole.WORKER, connection.getId(), input);
                connection.writeMessage(chat);
            }
    
    }
}
}
