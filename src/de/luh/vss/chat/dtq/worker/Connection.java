package de.luh.vss.chat.dtq.worker; 

import de.luh.vss.chat.common.Message;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Wraps a socket connection to the master and provides message send helpers.
 */
public final class Connection {


    
   private  final int myId ;//can modif the pin

    private   Socket socket =null;
    private  DataOutputStream out = null;
    private    DataInputStream in = null;

    private final String host;
    private final int port;

   

    /**
     * Creates a connection and immediately opens the socket.
     */
    public Connection(String host, int port,int myId) throws UnknownHostException, IOException  {
        this.host = host;
        this.port = port;
        this.myId = myId;
        connect();
       
            
      
    }

    /**
     * Opens the socket and initializes I/O streams.
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
     * Writes a message to the output stream and flushes immediately.
     */
    public synchronized void writeMessage(Message msg) throws IOException {
        msg.toStream(out);
        out.flush();
    }
    
}
