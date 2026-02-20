package de.luh.vss.chat.dtq.producer;

import de.luh.vss.chat.common.Message;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Socket wrapper for producer to master communication.
 */
public class ProducerConnection {
    private  final int myId ;//can modif the pin

    private   Socket socket =null;
    private  DataOutputStream out = null;
    private    DataInputStream in = null;

    private final String host;
    private final int port;

   

    /**
     * Creates a ProducerConnection.
     */
    public ProducerConnection(String host, int port,int myId) throws UnknownHostException, IOException  {
        this.host = host;
        this.port = port;
        this.myId = myId;
        connect();
       
            
      
    }

    /**
     * Performs connect.
     */
    public void connect() throws UnknownHostException, IOException{
     
        this.socket = new Socket(host, port);
        this.out = new DataOutputStream(socket.getOutputStream());
        this.in = new DataInputStream(socket.getInputStream());
    }
   
    /**
     * Returns socket.
     */
    public Socket getSocket(){return socket;}
    /**
     * Returns data Output Stream.
     */
    public DataOutputStream getDataOutputStream(){return out;}
    /**
     * Returns data Input Stream.
     */
    public DataInputStream getDataInputStream(){return in;}
    /**
     * Returns id.
     */
    public int getId(){return myId;}
    /**
     * Performs write Message.
     */
    public synchronized void writeMessage(Message msg) throws IOException {
        msg.toStream(out);
        out.flush();
    }

    /**
     * Sends chat.
     */
    public void sendChat(String msg) throws IOException {
        Message.ChatMessagePayload chat =
                new Message.ChatMessagePayload(de.luh.vss.chat.common.SystemRole.PRODUCER, myId, msg);
        writeMessage(chat);
    }

    /**
     * Sends job.
     */
    public void sendJob(Message.JobType jobType, String payload, String jobId) throws IOException {
        Message.ProducerSubmitJob job = new Message.ProducerSubmitJob(myId, jobType, payload, jobId);
        writeMessage(job);
    }
  
}
