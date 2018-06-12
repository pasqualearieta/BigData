import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
    	super(CustomWritable.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	
    	CustomWritable k1 = (CustomWritable)w1;
    	CustomWritable k2 = (CustomWritable)w2;
         
        int result = k1.getSolver().compareTo(k2.getSolver());
        if(0 == result) {
        	return 0;
        }
        else
        	return 1;
    }
}