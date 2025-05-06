/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * Utility class used by server to communicate with User Nodes
*/
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.io.File;

public class ServerSerializer{
	
    private static final int SERVER_VOTE_REQUEST = 0;
	private static final int SERVER_VOTE_OUTCOME = 1;

    /**
	 * Serialize status of commitment decision
	 * @param transactionId unique id for this 2P
	 * @param commit: true if the commit is occuring, false otherwise
	 * @return byte array containing userNodes information about commitment status
	 */
	public byte[] serializeOutcome(int transactionId, boolean commit) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);	

			dos.writeInt(SERVER_VOTE_OUTCOME);
			dos.writeInt(transactionId);
			dos.writeBoolean(commit);
			dos.flush();

        	return baos.toByteArray();
		}
		catch(Exception e){
			System.out.println("Failed serializeVoteRequest \n"); return new byte[0];
		}
	}

	/**
	 * Serialize information to be send to each userNodes
	 * @param transactionId unique id for this 2P
	 * @param img: byte array containing content of the collage
	 * @param sources: array of strings indicating the source images 
	 * @return byte array containing userNodes information about this collage update
	 */
	public byte[] serializeVoteRequest(int transactionId, byte[] img, List<String> userFiles) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);	

			dos.writeInt(SERVER_VOTE_REQUEST);
			dos.writeInt(transactionId);
			dos.writeInt(img.length);		
			dos.write(img);
			dos.writeInt(userFiles.size());	

			for(String file : userFiles)
				dos.writeUTF(file);

			dos.flush();
        	return baos.toByteArray();
		}
		catch(Exception e){System.out.println("Failed serializeVoteRequest \n"); return new byte[0];}
	}
	
	/**
	 * Helper function to parse source strings and extract participant information
	 * @param sources Array of strings in format "address:filename"
	 * @return Map containing participant addresses and their associated files
	 */
	public HashMap<String, List<String>> parseSourceAddresses(String[] sources) {		
		// Map to store adresses and their associated files
		HashMap<String, List<String>> addressToFiles = new HashMap<>();

		int NUM_STRING_PARTS = 2; 
		for (String source : sources){  // process each source string
			String[] parts = source.split(":", NUM_STRING_PARTS); 
			String address = parts[0];  // UserNode address
			String filename = parts[1]; // image filename
			
			if (!addressToFiles.containsKey(address)) 
				addressToFiles.put(address, new ArrayList<>());
		
			// get the list for this address and add filename
			addressToFiles.get(address).add(filename);  
		}
		return addressToFiles;	
	}
}