/**
 * @author Andrew Koulogeorge | 15440 CMU 
 * Utility class that stores data from transaction decision
*/

public class TransactionDecision {
    public boolean commitDecision;
    public String filename;
    public String imgPath;
    
    public TransactionDecision(boolean commitDecision, String filename, String imgPath) {
        this.commitDecision = commitDecision;
        this.filename = filename;
        this.imgPath = imgPath;
    }
}