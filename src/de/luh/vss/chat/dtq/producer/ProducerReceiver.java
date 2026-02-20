package de.luh.vss.chat.dtq.producer;

import de.luh.vss.chat.common.Message;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Receives messages from the master and enqueues them for handling.
 */
public class ProducerReceiver implements Runnable{
     private final ProducerConnection connection;
    private final BlockingQueue<Message> messagQueue;
    
    public boolean  stopReceiver = false;

    /**
     * Creates a ProducerReceiver.
     */
    public ProducerReceiver(ProducerConnection connection){
        this.connection = connection;
        this.messagQueue = new LinkedBlockingQueue<>();

    }
    
   /**
    * Receives value.
    */
   public void receive() throws IOException, ReflectiveOperationException, InterruptedException {
        
            Message response = Message.parse(connection.getDataInputStream());
            messagQueue.put(response);
            System.out.println("Producer received message: " + response.getClass().getSimpleName());
                    
        
    }
    /**
     * Returns queue.
     */
    public BlockingQueue<Message> getQueue(){return messagQueue;}
    
    /**
     * Runs the task loop.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted() && !stopReceiver) {
            try {
                receive();
            } catch (EOFException e) {
                System.out.println("Server closed connection (stopping receiver thread)");
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
