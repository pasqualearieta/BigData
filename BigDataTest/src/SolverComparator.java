import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SolverComparator extends WritableComparator {
    protected SolverComparator() {
    	super(CustomWritable.class, true);
    }   
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
    	
    	CustomWritable k1 = (CustomWritable)w1;
    	CustomWritable k2 = (CustomWritable)w2;
         
        int result = k1.getSolver().compareTo(k2.getSolver());
        if(0 == result) {
        	Double a = Double.parseDouble(k1.getValue().toString());
        	Double b = Double.parseDouble(k2.getValue().toString());

        	if(a > b)
        		return 1;
        	else if(a == b)
        		return 0;
        	else return -1;
        }
        return result;
    }
}