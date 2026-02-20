package de.luh.vss.chat.dtq.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * Holds worker connection metadata and lease timestamp.
 */
public class WorkerInfo{
    private final  int workerId;
    private final Socket workerSocket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    volatile long lastLease;

    /**
     * Creates a WorkerInfo.
     */
    public WorkerInfo(int workerId, Socket workerSocket, DataInputStream inputStream, DataOutputStream outputStream){
        this.workerId =workerId;
        this.workerSocket = workerSocket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        lastLease = System.currentTimeMillis();
    }

    

    /**
     * Returns worker Id.
     */
    public int getWorkerId() {
        return workerId;
    }
    /**
     * Returns worker Socket.
     */
    public Socket getWorkerSocket() {
        return workerSocket;
    }
    /**
     * Returns input Stream.
     */
    public DataInputStream getInputStream() {
        return inputStream; 
    }
    /**
     * Returns output Stream.
     */
    public DataOutputStream getOutputStream() {
        return outputStream;
    }
 
   
}   
