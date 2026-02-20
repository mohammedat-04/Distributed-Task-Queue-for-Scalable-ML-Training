package de.luh.vss.chat.dtq.worker;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.common.SystemRole;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Handles worker registration and periodic lease renewal with the master.
 */
public class WorkerServerConnection implements Runnable {
    /**
     * Manages the worker registration and periodic lease renewal with the master.
     */
    private final Connection connection;
    private final CountDownLatch registeredLatch = new CountDownLatch(1);
    private volatile boolean running = true;

    /**
     * Creates a WorkerServerConnection.
     */
    public WorkerServerConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * Blocks until registration has completed.
     */
    public void awaitRegistration() throws InterruptedException {
        registeredLatch.await();
    }

    /**
     * Returns true once the registration request has been sent successfully.
     */
    public boolean isRegistered() {
        return registeredLatch.getCount() == 0;
    }

    /**
     * Sends the initial registration message to the master.
     */
    private boolean register() {
        Message.ServiceRegistrationRequest request =
                new Message.ServiceRegistrationRequest(
                        SystemRole.WORKER,
                        connection.getId(),
                        connection.getSocket().getLocalAddress(),
                        connection.getSocket().getLocalPort()
                );

        try {
            connection.writeMessage(request);
       
            registeredLatch.countDown();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to register worker: " + e.getMessage());
            return false;
        }
    }

    /**
     * Registers the worker, then periodically renews the lease until stopped.
     */
    @Override
    public void run() {
        if (!register()) {
            System.err.println("Cannot continue without registration. Stopping WorkerServerConnection.");
            return;
        }

        

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                
                Thread.sleep(5_000); // renew every 5 seconds for quicker liveness detection
                Message.LeaseRenew lease = new Message.LeaseRenew(SystemRole.WORKER, connection.getId());
                connection.writeMessage(lease);
                //System.out.println("Lease renewed for worker ID " + connection.getId());
            } catch (InterruptedException e) {
                System.out.println("WorkerServerConnection interrupted");
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                System.err.println("Server closed connection or network error: " + e.getMessage());
                running = false; // stop loop
            }
        }

        System.out.println("WorkerServerConnection stopped");
    }
}
