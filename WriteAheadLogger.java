/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * Logging class that writes critical information to stable storage during transactions
*/

import java.io.IOException;
import java.util.Set;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.locks.ReentrantLock;


public class WriteAheadLogger{
    public static final String LOG_FILE = "wal.log";
    private final ReentrantLock lock;
    private final String IMG_PATH = "_img.bin";
    
    /* Constructor */
    public WriteAheadLogger() {
        this.lock = new ReentrantLock();
    
        try { // Create log file if it doesn't exist
            File logFile = new File(LOG_FILE);
            if (!logFile.exists()) 
                logFile.createNewFile();
        } 
        catch (IOException e) {System.err.println("Error creating log file: " + e.getMessage());}
    }

    /**
     * Log the list of participating user nodes in a transaction on server side
     * @param transactionId Unique ID for the transaction
     * @param participantIds Set of participant user node IDs
     */
    public void logUserList(int transactionId, Set<String> participantIds) {
        lock.lock();
        try {
            StringBuilder logEntry = new StringBuilder();
            String LOG_USER_LIST_CODE = "0";
            
            logEntry.append(transactionId).append(",");
            logEntry.append(LOG_USER_LIST_CODE).append(",");  
            logEntry.append(participantIds.size());
            
            // Add each participant ID
            for (String userId : participantIds) 
                logEntry.append(",").append(userId);
            
            // Add EOL marker to handle failures during recovery
            logEntry.append(",EOL");
            
            // Write to log file
            writeToLog(logEntry.toString());
        } finally { lock.unlock();}
    }
    
    /**
     * Save the collage image data to a file on server side
     * @param transactionId Unique ID for the transaction
     * @param img Byte array containing the collage image data
     */
    public void logCollage(int transactionId, byte[] img) {
        String imagePath = transactionId + IMG_PATH;
        try {
            FileOutputStream fos = new FileOutputStream(imagePath);
            fos.write(img);
            fos.flush();
            fos.close();
        } catch (IOException e) {
            System.err.println("Error writing collage image to file: " + e.getMessage());
        }
    }

    /**
     * Log the decision (commit or abort) for a transaction on server side
     * @param transactionId Unique ID for the transaction
     * @param commit The decision (true for commit, false for abort)
     * @param filename The name of the file being modified in this transaction
     */
    public void logServerDecision(int transactionId, boolean commit, String filename) {
        lock.lock();
        try {
            StringBuilder logEntry = new StringBuilder();
            String LOG_DECISION_CODE = "1";
            String imgPath = transactionId + IMG_PATH;
            
            logEntry.append(transactionId).append(",");
            logEntry.append(LOG_DECISION_CODE).append(",");  
            logEntry.append(commit).append(",");
            logEntry.append(filename).append(",");
            logEntry.append(imgPath);
            
            // Add EOL marker
            logEntry.append(",EOL");
            
            // Write to log file
            writeToLog(logEntry.toString());
        } finally { lock.unlock(); }
    }
    /**
     * Log the completion of the transaction on server side
     * @param transactionId Unique ID for the transaction
     */
    public void logServerCompleted(int transactionId) {
        lock.lock();
        try {
            StringBuilder logEntry = new StringBuilder();
            String LOG_COMPLETED_CODE = "2";
            
            logEntry.append(transactionId).append(",");
            logEntry.append(LOG_COMPLETED_CODE).append(",");  
    
            // Add EOL marker to handle failures during recovery
            logEntry.append("EOL");
            writeToLog(logEntry.toString());
            
        } 
        finally { lock.unlock();}
    }


    /**
     * Log commit for user Node along with files to be commited to transaction
     * @param transactionId: unique ID for the transaction
     * @param files: files to be commit if transaction completes
     */
    public void logUserNodeCommit(int transactionId, String[] files) {
        lock.lock();
        try {
            StringBuilder logEntry = new StringBuilder();
            String LOG_USER_COMMIT_CODE = "3";
            
            logEntry.append(transactionId).append(",");
            logEntry.append(LOG_USER_COMMIT_CODE).append(",");  
            logEntry.append(files.length);
            
            // Add each participant ID
            for (String file : files) 
                logEntry.append(",").append(file);
            
            // Add EOL marker to handle failures during recovery
            logEntry.append(",EOL");
            
            // Write to log file
            writeToLog(logEntry.toString());
        } finally { lock.unlock();}    
    }

    /**
     * Log the completion of the transaction on server side
     * @param transactionId Unique ID for the transaction
     */
    public void logUserNodeCompleted(int transactionId) {
        lock.lock();
        try {
            StringBuilder logEntry = new StringBuilder();
            String LOG_USER_COMPLETED_CODE = "4";
            
            logEntry.append(transactionId).append(",");
            logEntry.append(LOG_USER_COMPLETED_CODE).append(","); 
    
            // Add EOL marker to handle failures during recovery
            logEntry.append("EOL");
            writeToLog(logEntry.toString());
            
        } 
        finally { lock.unlock();}
    }    

    /**
     * Helper function that appends information onto write ahead log
     * @param data: string representing information for this log entry
     */
    private void writeToLog(String data){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(data);
            writer.newLine();
            writer.flush();
        } 
        catch (IOException e) { System.err.println("Error writing user list to log: " + e.getMessage());}
    }
}