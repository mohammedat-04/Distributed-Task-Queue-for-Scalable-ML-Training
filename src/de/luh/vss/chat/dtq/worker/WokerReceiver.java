package de.luh.vss.chat.dtq.worker;

import de.luh.vss.chat.common.Message;
import java.io.EOFException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Reads messages from the master and enqueues them for processing.
 */
public class WokerReceiver implements Runnable {
    private final Connection connection;
    private final BlockingQueue<Message> messagQueue;
    
    public boolean  stopReceiver = false;

    /**
     * Creates a WokerReceiver.
     */
    public WokerReceiver(Connection connection,BlockingQueue<Message> queue){
        this.connection = connection;
        this.messagQueue = queue;

    }
    
    /**
     * Reads a single message from the connection and enqueues it.
     */
    public void receive() throws IOException, ReflectiveOperationException, InterruptedException {
        
            Message response = Message.parse(connection.getDataInputStream());
            messagQueue.put(response);
    }
    /**
     * Returns queue.
     */
    public Queue<Message> getQueue(){return messagQueue;}
    
    /**
     * Continuously reads messages until interrupted or the connection closes.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted() && !stopReceiver) {
            try {
                receive();
            } catch (EOFException e) {
                System.out.println("Server closed connection (stopping receiver thread)");
                Thread.currentThread().interrupt();
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException | ReflectiveOperationException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
