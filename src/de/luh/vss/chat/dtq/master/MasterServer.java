package de.luh.vss.chat.dtq.master;

import de.luh.vss.chat.common.Message;
import de.luh.vss.chat.common.SystemRole;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Accepts producer/worker connections, registers peers, and enqueues messages for the master.
 */
public class MasterServer implements Runnable {
    private static final String C_RESET = "\u001B[0m";
    private static final String C_GREEN = "\u001B[32m";
    private static final String C_RED = "\u001B[31m";
    
    private  int port;
    private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
    private final ExecutorService clientPool = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<Integer, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ProducerInfo> producers = new ConcurrentHashMap<>(); 


    /**
     * Creates a MasterServer.
     */
    public MasterServer(int port) throws IOException{
        this.port = port;
        
 
    }
   /**
    * Registers the peer and forwards non lease messages into the master queue.
    */
   private void handleClient(Socket socket) throws ReflectiveOperationException {
    Integer peerId = null;
    SystemRole role = null;
    String remote = String.valueOf(socket.getRemoteSocketAddress());

    try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
         DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

        // Registration
        Message message = Message.parse(in);
        if (!(message instanceof Message.ServiceRegistrationRequest reg)) {
            System.err.println("[" + remote + "] no registration");
            new Message.ServiceErrorResponse("First message must be SERVICE_REGISTRATION_REQUEST").toStream(out);
            return;
        }

        role = reg.getRole();
        peerId = reg.getId();

        if (role == SystemRole.WORKER) {
            if (isWorkerRegistered(peerId)) {
                new Message.ServiceErrorResponse("Worker with this ID is already registered.").toStream(out);
                return;
            }
            workers.put(peerId, new WorkerInfo(peerId, socket, in, out));
            System.out.println(C_GREEN + "[" + remote + "] Worker registered with id=" + peerId + C_RESET);
          
            
        } else if (role == SystemRole.PRODUCER) {
            if (isProducerRegistered(peerId)) {
                new Message.ServiceErrorResponse("Producer with this ID is already registered.").toStream(out);
                return;
            }
            producers.put(peerId, new ProducerInfo(peerId, socket, in, out));
            System.out.println(C_GREEN + "[" + remote + "] Producer registered with id=" + peerId + C_RESET);
        }
        
        // Main loop
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Message msg = Message.parse(in);
                if (msg instanceof Message.LeaseRenew lease) {
                     // update immediately here
                    if(lease.getRole() == SystemRole.WORKER){
                        WorkerInfo worker = workers.get(lease.getId());
                        if(worker != null) worker.lastLease = System.currentTimeMillis();
                    } else if(lease.getRole() == SystemRole.PRODUCER){
                        ProducerInfo producer = producers.get(lease.getId());
                        if(producer != null) producer.lastLease = System.currentTimeMillis();
                    }
            } else if(msg instanceof Message.ServiceRegistrationRequest req){
                System.err.println("i recieve registartion form"+req.getRole()+req.getId());
            }
            else {
                messageQueue.put(msg); // other messages
            }
            } catch (EOFException | SocketException e) {
                System.out.println(C_RED + "[" + remote + "] disconnected id=" + peerId + " role=" + role + C_RESET);
                if(role == SystemRole.WORKER)workers.remove(peerId);
                else{producers.remove(peerId);}
                break;
            } catch (ReflectiveOperationException | IOException e) {
                System.err.println("Error parsing message: " + e.getMessage());
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    } catch (IOException e) {
        System.err.println("Connection error: " + e.getMessage());
    }
}

    /**
     * Returns message Queue.
     */
    public BlockingQueue<Message> getMessageQueue(){
        return this.messageQueue;
    }   
    /**
     * Returns workers.
     */
    public ConcurrentHashMap<Integer, WorkerInfo> getWorkers(){
        return this.workers;
    }
    /**
     * Returns producers.
     */
    public ConcurrentHashMap<Integer, ProducerInfo> getProducers(){
        return this.producers;
    }
  
    
    /**
     * Accepts client connections and dispatches each to a handler thread.
     */
    @Override
    public void run() {
       
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            port = serverSocket.getLocalPort();
            System.out.println("Master Server listening on port " + port);
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = serverSocket.accept();
                clientPool.submit(() -> {
                    try {
                        handleClient(socket);
                    } catch (ReflectiveOperationException e) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Master Server stopped (IO error):");
        }
    }

    /**
     * Returns whether worker Registered.
     */
    private boolean isWorkerRegistered(int workerId) {
        return workers.containsKey(workerId);
    }
    /**
     * Returns whether producer Registered.
     */
    private boolean isProducerRegistered(int producerId) {
        return producers.containsKey(producerId);
    }
    
}
