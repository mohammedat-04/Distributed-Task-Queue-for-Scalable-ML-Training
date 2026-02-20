package de.luh.vss.chat.dtq.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * Holds producer connection metadata and lease timestamp.
 */
class ProducerInfo{
    private final int producerId;
    private final Socket producerSocket;
    private final DataInputStream inputStream;
    private final DataOutputStream outputStream;
    volatile long lastLease;



    /**
     * Creates a ProducerInfo.
     */
    public ProducerInfo(int producerId, Socket producerSocket, DataInputStream inputStream, DataOutputStream outputStream){
        this.producerId =producerId;
        this.producerSocket = producerSocket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        lastLease = System.currentTimeMillis();
    }

    /**
     * Returns producer Id.
     */
    public int getProducerId() {
        return producerId;
    }
    /**
     * Returns producer Socket.
     */
    public Socket getProducerSocket() {
        return producerSocket;
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
