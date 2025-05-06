/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * Utility class used by UserNode to communicate with Coordinator
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class UserNodeSerializer{
    
    private static final int USER_VOTE_RESPONSE = 2;
	private static final int USER_VOTE_ACK = 3;
    private static String myId;
    
    // server adress constants
	private static final String SERVER_ADDY = "Server";

    /* Constructor */
    public UserNodeSerializer(String id){ myId = id; }
    
    /**
     * Send vote back to coordinator
     * @param transactionId ID of the transaction
     * @param vote true for YES, false for NO
     */
    public void sendVote(ProjectLib PL, int transactionId, boolean vote) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            dos.writeInt(USER_VOTE_RESPONSE); 
            dos.writeInt(transactionId);
            dos.writeBoolean(vote);
            dos.flush();
            
            ProjectLib.Message msg = new ProjectLib.Message(SERVER_ADDY, baos.toByteArray());
            PL.sendMessage(msg);
        } catch (Exception e) { System.out.println("Failed to send vote: " + e.getMessage());}
    }
    
    /**
     * Send acknowledgment back to coordinator after the usernode commit
     * @param transactionId ID of the transaction
     */
    public void sendAck(ProjectLib PL, int transactionId) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            dos.writeInt(USER_VOTE_ACK); // Message ID for ack message
            dos.writeInt(transactionId);
            dos.writeUTF(myId);
            dos.flush();
            
            ProjectLib.Message msg = new ProjectLib.Message(SERVER_ADDY, baos.toByteArray());
            PL.sendMessage(msg);
        } catch (Exception e) {System.out.println("Failed to send acknowledgment: " + e.getMessage());}
    }

}