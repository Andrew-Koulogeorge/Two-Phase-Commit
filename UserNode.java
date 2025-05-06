/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * UserNode acts as the clients in a 2PC protocol
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

public class UserNode implements ProjectLib.MessageHandling {

	// constants used for identifying message type in communication protocol between the coordinator and user nodes
	private static final int SERVER_VOTE_REQUEST = 0;
	private static final int SERVER_VOTE_OUTCOME = 1;

	// node identifer and communication/simulation library
	public static String myId;
	private static ProjectLib PL;

	// write ahead logger for fault tolerance
	private static WriteAheadLogger logger;

	// Serializer for communication protocol
	private static UserNodeSerializer userNodeSerializer;

	// Concurrency-safe data structures for transaction management
	private HashSet<String> lockedFiles = new HashSet<>();
	private ConcurrentHashMap<Integer, List<String>> transactionFiles = new ConcurrentHashMap<>();
	private final ReentrantLock fileLock = new ReentrantLock();

	/* UserNode Constructor */
	public UserNode( String id ) { 
		myId = id; 
		logger = new WriteAheadLogger();
		userNodeSerializer = new UserNodeSerializer(myId);
		}


	/**
	 * Function called when a message is recv by the userNode
	 * There are two scenarios where a message will be coming from the Coordinator
	 * to the user node: (1) Vote Request (2) Outcome Notify
	 * If function returns false, the message is placed into the queue
	 * @param msg: Message object sent by the coordinator
	 * @return true if we accapted this message, false otherwise. 
	 */
	public boolean deliverMessage( ProjectLib.Message msg ) {
		try{
			// data structure for handling serialized data
			ByteArrayInputStream bais = new ByteArrayInputStream(msg.body);
			DataInputStream dis = new DataInputStream(bais);		

			// extract header information from msg
			int msgType = dis.readInt();
			int transactionId = dis.readInt();		

			// handle different types of msgs
			switch(msgType){
				case SERVER_VOTE_REQUEST:
					handleVoteRequest(dis, transactionId);
					break;
				case SERVER_VOTE_OUTCOME:
					handleVoteOutcome(dis, transactionId);
					break;
				default: 
					return false;
			}
			return true;
		}
		catch (IOException e) {
            System.out.printf("%s: Failed to process message: %s\n", myId, e.getMessage());
            return false;
        }
	}


	/**
	 * Helper function used to handle vote requests from Coordinator
	 * @param dis: data stream containing serialized information from the coordinator
	 * @param transactionId: unique id for transaction currently underway
	 */
	public void handleVoteRequest(DataInputStream dis, int transactionId){
			try{
				// Deserialize image 
				int imgLength = dis.readInt();
				byte[] img = new byte[imgLength];
				dis.readFully(img);			
		
				// Deserialize file list
				int numFiles = dis.readInt();
				String[] files = new String[numFiles];
				for (int i = 0; i < numFiles; i++) 
					files[i] = dis.readUTF();
				
				// Handle vote outcome
				boolean vote = PL.askUser(img, files); 						
				fileLock.lock();
				
				// ensure files not locked
				if(vote) vote = checkFiles(transactionId, files);			

				/* write transaction id, vote status to ss with fsync */
				if (vote){
					logger.logUserNodeCommit(transactionId, files);
					PL.fsync();
				} 
				
				// lock files and send vote back to Coordinator
				lockFiles(transactionId, files);							
				userNodeSerializer.sendVote(PL, transactionId, vote);		
				fileLock.unlock();
			}
			catch(Exception e)
			{
			System.out.println("Failed handleVoteRequest \n");
			System.out.println(e.getMessage());

			return;
			}
	}


	/**
	 * Helper function used to handle vote outcome from Coordinator
	 * @param dis: data stream containing serialized information from the coordinator
	 * @param transactionId: unique id for transaction currently underway
	 */
	public void handleVoteOutcome(DataInputStream dis, int transactionId){
		try{
			boolean commit = dis.readBoolean();
			if(commit) deleteFiles(transactionId);
			else removeStagingGround(transactionId);
			userNodeSerializer.sendAck(PL, transactionId);	

			/* write to ss transactionId and status of finished commitment */
			logger.logUserNodeCompleted(transactionId);
			PL.fsync();
		}
		catch (IOException e) {System.out.printf("%s: Failed handleVoteOutcome: %s\n", myId, e.getMessage());}
	}	

	/**
	 * Helper function to ensure files needed for this transaction are avalible. 
	 * @param transactionId: unique id for transaction currently underway	 
	 * @param files: list of file paths that are involved in this transaction
	 * @return true if no files active in this trasaction are currently locked, false otherwise
	 */
	public boolean checkFiles(int transactionId, String[] files){
		int numFiles = files.length;
		// ensure that none of the files we are working with are locked
		for (int i = 0; i < numFiles; i++){
			if(lockedFiles.contains(files[i]))
				return false;
		}	
		return true;
	}	

	/**
	 * Helper function to lock files before sending a yes vote back to the coordinator
	 * @param transactionId: unique id for transaction currently underway
	 * @param files: list of file paths that are involved in this transaction
	 */
	public void lockFiles(int transactionId, String[] files){
		// if none of the files for this transaction are locked, lock them
		for (int i = 0; i < files.length; i++)
			lockedFiles.add(files[i]); 
		
		// record files active in this transaction
		List<String> filesList = Arrays.asList(files);
		transactionFiles.put(transactionId, filesList);	
	}
	
	/**
	 * Helper function to remove files from staging ground in event transaction was aborted
	 * Want these files to be avalible for other transactions
	 * NOTE: An abort message may be send to the user even if no commitment was made. Thus, we check
	 * before removing if any files exist for this transaction
	 * @param transactionId: unique id for transaction currently underway
	 */
	public void removeStagingGround(int transactionId){
		// remove images from staging ground
		List<String> files = transactionFiles.getOrDefault(transactionId, null);
		if(files == null) return;
		for (String file : files){
			if(lockedFiles.contains(file)) 
				lockedFiles.remove(file);
		}		
	} 
	
	
	/**
	 * Helper function to delete files locally on commited transaction
	 * NOTE: A commit message may be send to the user several times in the case where 
	 * the server crashes after it sends vote results but before it gets them back
	 * Thus, we must check if the files exist before deleting them
	 * @param transactionId: unique id for transaction currently underway
	 */
	public void deleteFiles(int transactionId){
		// delete local files if they exist (want to be idempotent)
		List<String> files = transactionFiles.getOrDefault(transactionId, null);
		if(files == null) return; 
		for (String file : files) 
			deleteFile(file);			
	}

	/**
	 * Helper function to delete file from the file system after a commitment 
	 * @param filename: file to be deleted
	 */
	private void deleteFile(String filename){
		File fileObj = new File(filename);
		if (fileObj.exists()) {
			try { boolean deleted = fileObj.delete();} 
			catch (Exception e) { System.out.println("Error deleting file " + filename + ": " + e.getMessage());}
		}		
	}

	/**
	 * Failure recovery code that traverses write-ahead log in stable storage to ensure
	 * consistent state in the event of a failure. This code is executed each time that a User Node
	 * spins up
	 * 
	 * First we traverse the log to record the outcome of each UserNode Transaction. Then we
	 * retore the User Nodes state based on the transaction statuses
	 */
	public void traverseWriteAheadLog(){
		HashMap<Integer, Integer> transactionStatuses = new HashMap<>(); // transaction id -> most recent command (3/4)
		
		// Constants for log entry types
		final int LOG_COMMIT = 3;
		final int LOG_FINISH = 4;

		try {
			File file = new File(logger.LOG_FILE);
			
			BufferedReader reader = new BufferedReader(new FileReader(logger.LOG_FILE));
			String line;
			while ((line = reader.readLine()) != null) {
				// Ensure line is complete (has EOL marker)
				if (!line.endsWith("EOL")) { continue; }
				
				// Parse the log entry
				String[] parts = line.split(",");
				int transactionId = Integer.parseInt(parts[0]);
				int logType = Integer.parseInt(parts[1]);
				
				// Update transaction status with most recent command
				transactionStatuses.put(transactionId, logType);
				
				switch (logType) { 
					case LOG_COMMIT: // Log format: transactionId,3,num_files,file1,file2, ... ,EOL
						int fileCount = Integer.parseInt(parts[2]);
						List<String> files = new ArrayList<>();
						for (int i = 0; i < fileCount && i + 3 < parts.length - 1; i++) 
							files.add(parts[i + 3]);
						transactionFiles.put(transactionId, files); 
						break;

					case LOG_FINISH: // Log format: transactionId,4,EOL
						break;
				}
			}
			reader.close();
		} 
		catch (IOException e) {System.err.println("Error reading log file: " + e.getMessage());} 

		// after processing logs, apply recovery logic based on data gathered
		restoreState(transactionStatuses);
	}	

	/**
	 * Based on intentions recorded in the logs, restore commited state for user node
	 * @param transactionStatus: transaction id -> most recent command found in log
	 */
	public void restoreState(HashMap<Integer, Integer> transactionStatus){
		
		// Constants for log entry types
		final int LOG_COMMIT = 3;
		final int LOG_FINISH = 4;
		
		// Process each transaction for recovery
		for (Integer transactionId : transactionStatus.keySet()) {
			int latestLogType = transactionStatus.get(transactionId);
			System.out.printf("Recovering transaction %d with latest log type %d\n", transactionId, latestLogType);
			
			// Case 1: Transaction completed successfully, nothing to do
			if (latestLogType == LOG_FINISH) { continue; }
			
			// Case 2: Commitment occured without finishing transaction
			else if (latestLogType == LOG_COMMIT) {
				List<String> files = transactionFiles.get(transactionId);
				if (files != null) {
					lockFiles(transactionId, files.toArray(new String[0]));									
					userNodeSerializer.sendVote(PL, transactionId, true);										
				}
			}	
		}
	}	

	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		int port = Integer.parseInt(args[0]);
		String myId = args[1];
		UserNode UN = new UserNode(myId);
		PL = new ProjectLib( port, myId, UN );

		/* Crash Recovery Logic */
		UN.traverseWriteAheadLog();
	}
}