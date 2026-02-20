package de.luh.vss.chat.dtq.producer;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.common.SystemRole;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles producer registration and periodic lease renewal with the master.
 */
public class ProducerServerConnection implements Runnable{
     private final ProducerConnection connection; 
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch registeredLatch = new CountDownLatch(1);

    /**
     * Creates a ProducerServerConnection.
     */
    public ProducerServerConnection(ProducerConnection connection){
        this.connection = connection;
    }

    /**
     * Stops value.
     */
    public void stop() {
        
        Thread.currentThread().interrupt();
    }

    /**
     * Performs await Registration.
     */
    public void awaitRegistration() throws InterruptedException {
        registeredLatch.await();
    }

    /**
     * Performs register.
     */
    public void register() throws IOException {
        Message.ServiceRegistrationRequest request =new Message.ServiceRegistrationRequest(SystemRole.PRODUCER,
                        connection.getId(), 
                            connection.getSocket().getLocalAddress(),
                                connection.getSocket().getLocalPort());
                
            connection.writeMessage(request);
            registeredLatch.countDown();
            
      
        
    }
    /**
     * Returns is Connected.
     */
    public boolean  getIsConnected(){return running.get();}
    /**
     * Runs the task loop.
     */
    @Override
    @SuppressWarnings("CallToPrintStackTrace")
    public void run() {
        try {
            register();
           
            while (!Thread.currentThread().isInterrupted()) { 
                 Message.LeaseRenew lease = new Message.LeaseRenew(SystemRole.PRODUCER,connection.getId());
                running.set(true);
                Thread.sleep(5_000); /// lease renew every 5 sec
               connection.writeMessage(lease);
              
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
