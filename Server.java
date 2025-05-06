/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * Server process that acts as the coordinator node in a 2PC 
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
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class Server implements ProjectLib.CommitServing {

	/* constants for establishing communication protocol between the coordinator and the users */
	private static final int ABORT = -1;
	private static final int SERVER_VOTE_REQUEST = 0;
	private static final int SERVER_VOTE_OUTCOME = 1;
	private static final int USER_VOTE_RESPONSE = 2;
	private static final int USER_VOTE_ACK = 3;

	private static final long TIMEOUT_MS = 3000; 																				// 3 seconds timeout
	private static final int MAX_RETRIES = 20;																					// max number of retransmissions for collecting acks
	private static AtomicInteger transactionId = new AtomicInteger(0); 										   					// unique IDs to keep track of transactions
	private static ConcurrentHashMap<Integer, Integer> voteCounter = new ConcurrentHashMap<>(); 								// keep track of yes votes from userNodes
	private static ConcurrentHashMap<Integer, ConcurrentHashMap<String,Boolean>> globalAckTracker = new ConcurrentHashMap<>(); 	// keep track of which user nodes responded
	private static ProjectLib PL;
	private static WriteAheadLogger logger;
	private static ServerSerializer serverSerializer;

	/* Server Constructor for logger init */
	public Server() { 
		logger = new WriteAheadLogger(); 
		serverSerializer = new ServerSerializer();
		}

	/* Class that implements ProjectLib.Messagehandling interface to handle async communication */
	private class MessageHandler implements ProjectLib.MessageHandling{

		/**
		 * Function called when message received by coordinator
		 * If false is returned by this method, the message is placed back into the queue
		 * There are two types of messages that could be coming from the userNodes:
		 * - answers to a vote where the user is saying they are willing to commit or not
		 * - ACKs after a commit has already been declared by the Coordinator
		 * @param msg: Message object sent by one of the userNodes
		 * @return true if we accapted this message, false otherwise. 
		 */
		public boolean deliverMessage(ProjectLib.Message msg){

			byte[] body = msg.body; 
        	ByteArrayInputStream bais = new ByteArrayInputStream(body);
        	DataInputStream dis = new DataInputStream(bais);
			try {
				int msgType = dis.readInt();
				int transactionId = dis.readInt();

				if(msgType == USER_VOTE_RESPONSE){ 						  // CASE 1: msg contains user vote			
					boolean vote = dis.readBoolean();			
					if(vote) voteCounter.put(transactionId, voteCounter.getOrDefault(transactionId, 0) + 1); // count vote
					else voteCounter.put(transactionId, ABORT);								     			 // signal abort of transaction
					return true; 	
				}
				else if(msgType == USER_VOTE_ACK){ 						  // CASE 2: msg contains user ACK
					String userId  = dis.readUTF();		
					globalAckTracker.get(transactionId).put(userId,true); // mark this user as ACKed (idempotent!)
					return true;
				}
			}
			catch(Exception e){ System.out.println("Failed serializeVoteRequest:" + e.getMessage() + "\n"); return false; }
			return false; 
		}
	}
	
	/**
	 * Function called by the simulation code to spawn a thread to handle a 2PC
	 * 
	 * @param filename: name of file where collage is stored on server
	 * @param img: byte array containing content of the collage
	 * @param sources: array of strings indicating the source images 
	 * The sources array is of the format node_id:path_to_file
	 */
	public void startCommit( String filename, byte[] img, String[] sources ) {		
		// Initialize the ACK tracking for this transaction before commitment begins
		
		// spawn thread to handle execution
		int cpyId = transactionId.incrementAndGet();
		new Thread(() -> {
			try {execute2PC(cpyId, filename, img, sources);} 
			catch (Exception e) { return; }
		}).start();	
		
		// establish ack tracker for collecting acks from user nodes
		ConcurrentHashMap<String, Boolean> ackTracker = new ConcurrentHashMap<>();
		globalAckTracker.put(cpyId, ackTracker);	
	}

	/**
	 * Function executed by a thread to execute the 2PC
	 * @param transactionId: unique id for this 2PC
	 * @param filename: name of file where collage is stored on server
	 * @param img: byte array containing content of the collage
	 * @param sources: array of strings indicating the source images 
	 */
	public void execute2PC(int transactionId, String filename, byte[] img, String[] sources){
		
		// parse addresses from sources to get ids of user nodes in this 2PC
		HashMap<String, List<String>> participantFiles = serverSerializer.parseSourceAddresses(sources);		
    	
		// Get list of unique participant addresses
		Set<String> participantIds = participantFiles.keySet();							    		

		logger.logUserList(transactionId, participantIds);
		PL.fsync();

		/* VOTE DISTRIBUTION PHASE: seralize args and send to each user */
		distributeVotes(participantFiles, transactionId, img);

		/* VOTE REC PHASE; loop until all userNodes confirmed 2PC or one rejects */
		boolean commitDecision = collectVotes(participantIds.size(), transactionId);
		if(commitDecision) logger.logCollage(transactionId, img);
		logger.logServerDecision(transactionId, commitDecision, filename);
		PL.fsync();

		// write this image to server working directory		
		if(commitDecision) writeCollage(filename, img); 	

		/* DISTRIBUTE DECISION PHASE */
		distributeDecision(participantIds, transactionId, commitDecision);
		
		/* COLLECT USER ACKS: resend outcomes if transaction occured */
		collectAcks(participantIds, transactionId, commitDecision);

		/* record that transaction completed */
		logger.logServerCompleted(transactionId);
		PL.fsync();
	}

	/**
	 * Helper function to send out votes to all user nodes
	 * @param participantFiles: hashmap containing each user node id and the files involved for this 2PC 
	 * @param transactionId: unique id for this 2PC
	 * @param img: byte array containing content of the collage
	 */
	public void distributeVotes(HashMap<String, List<String>> participantFiles, int transactionId, byte[] img){
		for (String userId: participantFiles.keySet()){
			byte[] payload = serverSerializer.serializeVoteRequest(transactionId, img, participantFiles.get(userId)); 
			ProjectLib.Message msg = new ProjectLib.Message( userId, payload );
			PL.sendMessage( msg );
		} 				
	}

	/**
	 * Helper function to collect votes from user nodes 
	 * @param votesNeeded: number of user nodes active in this voting round
	 * @param transactionId: unique id for this 2PC
	 * @return true if votes all came in YES and false if at least one vote returned ABORT
	 */
	public boolean collectVotes(int votesNeeded, int transactionId){
		long startTime = System.currentTimeMillis();			
		while(voteCounter.getOrDefault(transactionId, 0) < votesNeeded){
			
			// a userNode said no; transaction aborted
			if(voteCounter.getOrDefault(transactionId, 0) == ABORT)
				return false;						
			
			// if its been longer then 3 seconds, abort
			if (System.currentTimeMillis() - startTime > TIMEOUT_MS)
				return false;
		}
		return true;
	}

	/**
	 * Helper function called on commitDecisionment to write img to coordinator directory
	 * @param filename: location on server directory to write collage
	 * @param img: byte array containing collage content
	 */
	public void writeCollage(String filename, byte[] img){
		try{
			java.io.FileOutputStream fos = new java.io.FileOutputStream(filename);
			fos.write(img);
			fos.close();
		} 
		catch (java.io.IOException e) {System.out.println("Failed to write collage to coordinator directory \n");}	
	}

	/**
	 * Helper method to restore a collage image from stable storage
	 * @param filename destination filename
	 * @param imgPath path to the stored image in stable storage
	 */
	private void restoreCollageFromStorage(String filename, String imgPath) {
		try {
			File imgFile = new File(imgPath);
			if (imgFile.exists()) {
				byte[] img = Files.readAllBytes(Paths.get(imgPath));
				writeCollage(filename, img);
			} 
		} 
		catch (IOException e) {System.err.println("Error restoring collage from storage: " + e.getMessage());}
	}	

	/**
	 * Helper function to send out votes to all user nodes
	 * @param participantIds: set containing each user node id
	 * @param transactionId: unique id for this 2PC
	 * @param img: byte array containing content of the collage
	 */
	public void distributeDecision(Set<String> participantIds, int transactionId, boolean commitDecision){
		for (String userId: participantIds){
			byte[] payload = serverSerializer.serializeOutcome(transactionId, commitDecision); 
			ProjectLib.Message msg = new ProjectLib.Message( userId, payload );
			PL.sendMessage( msg );
		} 			
	}

	/**
	 * Helper function to collect acks from usernodes from 2PC
	 * Function only called when outcome of distributed commit was to commit
	 * @param participantIds: set of ids for each user in the 2PC
	 * @param transactionId: unique id for this 2PC
	 */
	public void collectAcks(Set<String> participantIds, int transactionId, boolean commitDecision){
		int acksNeeded = participantIds.size();
		int acksRecv = 0;
		int numRetrys = 0;
		ConcurrentHashMap<String, Boolean> ackTracker = globalAckTracker.get(transactionId);
		long startTime = System.currentTimeMillis();

		while(true){
			for (Boolean ack: ackTracker.values()){
				if(ack) acksRecv++;
			}
			if(acksRecv == acksNeeded) return; 
			else acksRecv = 0;
			
			/* HANDLE RETRANSMISION */
			if (System.currentTimeMillis()-startTime > TIMEOUT_MS) {
				for (String userId: participantIds){
					if(!ackTracker.containsKey(userId)){
						byte[] payload = serverSerializer.serializeOutcome(transactionId, commitDecision); 
						ProjectLib.Message msg = new ProjectLib.Message( userId, payload );
						PL.sendMessage( msg );
					}
				}
				numRetrys++;			
				if(numRetrys == MAX_RETRIES) return;
				startTime = System.currentTimeMillis();
			}
		}		
	}	

	/**
	 * Failure recovery code that traverses write-ahead log in stable storage to ensure
	 * consistent state in the event of a failure
	 */
	public void traverseWriteAheadLog() {
		// transaction id -> most recent command
		HashMap<Integer, Integer> transactionStatus = new HashMap<>(); 					
		// transaction id -> user nodes
		HashMap<Integer, Set<String>> transactionParticipants = new HashMap<>(); 		
		// transaction id -> decision object
		HashMap<Integer, TransactionDecision> transactionDecisions = new HashMap<>(); 	
		
		// Constants for log entry types
		final int LOG_USER_LIST = 0;
		final int LOG_DECISION = 1;
		final int LOG_COMPLETED = 2;
		
		try {
			File file = new File(logger.LOG_FILE);
			if (!file.exists()) {System.out.println("No WAL log file found. Starting with clean state."); return;}
			
			BufferedReader reader = new BufferedReader(new FileReader(logger.LOG_FILE));
			String line;
			while ((line = reader.readLine()) != null) {
				// Check if line is complete (has EOL marker)
				if (!line.endsWith("EOL")) { continue; }
				
				// Parse the log entry
				String[] parts = line.split(",");
				int transactionId = Integer.parseInt(parts[0]);
				int logType = Integer.parseInt(parts[1]);
				
				// Update transaction status with most recent command
				transactionStatus.put(transactionId, logType);
				
				// Process based on log type
				switch (logType) { 
					case LOG_USER_LIST: // Format: transactionId,0,participantCount,participant1,participant2,...,EOL
						int participantCount = Integer.parseInt(parts[2]);
						Set<String> participants = new HashSet<>();
						for (int i = 0; i < participantCount && i + 3 < parts.length - 1; i++) 
							participants.add(parts[i + 3]);
						transactionParticipants.put(transactionId, participants);
						break;

					case LOG_DECISION: // Format: transactionId,1,commit(true/false),filename,imgPath,EOL
						boolean commitDecision = Boolean.parseBoolean(parts[2]);
						String filename = parts[3];
						String imgPath = parts[4];
						
						TransactionDecision decision = new TransactionDecision(commitDecision, filename, imgPath);
						transactionDecisions.put(transactionId, decision);
						break;
						
					case LOG_COMPLETED: // Format: transactionId,2,EOL
						break;
				}
			}
			reader.close();
		} 
		catch (IOException e) {System.err.println("Error reading log file: " + e.getMessage());} 

		// after processing logs, apply recovery logic based on data gathered
		restoreState(transactionStatus, transactionParticipants, transactionDecisions);
	}

	/**
	 * Based on intentions recorded in the logs, restore commited state
	 * @param transactionStatus: transaction id -> most recent command found in log
	 * @param transactionParticipants: transaction id -> user nodes active in transaction
	 * @param transactionDecisions: transaction id -> transaction decision information object
	 */
	public void restoreState(HashMap<Integer, Integer> transactionStatus, HashMap<Integer, Set<String>> transactionParticipants,
									HashMap<Integer, TransactionDecision> transactionDecisions){
		// Constants for log entry types
		final int LOG_USER_LIST = 0;
		final int LOG_DECISION = 1;
		final int LOG_COMPLETED = 2;
		
		// Process each transaction for recovery
		for (Integer transactionId : transactionStatus.keySet()) {
			int latestLogType = transactionStatus.get(transactionId);
			
			// Case 1: Transaction completed successfully, nothing to do
			if (latestLogType == LOG_COMPLETED) continue;
			
			// Case 2: Only participant list was logged or transaction was aborted
			if (latestLogType == LOG_USER_LIST || 
				(latestLogType == LOG_DECISION && 
				!transactionDecisions.get(transactionId).commitDecision)) {
				
				// Send abort messages to all participants to ensure they unlock resources
				Set<String> participantIds = transactionParticipants.get(transactionId);
				if (participantIds != null) distributeDecision(participantIds, transactionId, false);
			}
			
			// Case 3: Decision was committed but might not have completed
			else if (latestLogType == LOG_DECISION && transactionDecisions.get(transactionId).commitDecision) {
				
				TransactionDecision decision = transactionDecisions.get(transactionId);
				if (decision != null) {
					// Restore collage image from storage
					restoreCollageFromStorage(decision.filename, decision.imgPath);
					
					// Resend commit messages to participants and collect ACKs
					Set<String> participantIds = transactionParticipants.get(transactionId);
					if (participantIds != null) {
						// Set up ACK tracking for this transaction
						ConcurrentHashMap<String, Boolean> ackTracker = new ConcurrentHashMap<>();
						globalAckTracker.put(transactionId, ackTracker);
						
						// Resend commit outcome to all participants
						for (String participant : participantIds)
							distributeDecision(participantIds, transactionId, true);
						
						// collect ACKs asynchronously
						collectAcks(participantIds, transactionId, true);
					}
				}
			}
		}
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		int port = Integer.parseInt(args[0]);
		Server srv = new Server();
		PL = new ProjectLib(port, srv, srv.new MessageHandler()); 
		
		/* Crash Recovery Logic */
		srv.traverseWriteAheadLog();
	}
}