import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomWritable implements WritableComparable<CustomWritable> {

	private Text solver;
	private Text time;

	protected CustomWritable() {
		solver = new Text("");
		time = new Text("");
	}

	protected CustomWritable(Text tag, Text value) {
		this.solver = tag;
		this.time = value;

	}

	public Text getSolver() {
		return solver;
	}

	public Text getValue() {
		return time;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		solver.readFields(arg0);
		time.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		solver.write(arg0);
		time.write(arg0);
	}

	public static CustomWritable read(Text tag, Text value) throws IOException {
		CustomWritable w = new CustomWritable(tag, value);
		return w;
	}

	@Override
	public int compareTo(CustomWritable o) {
		int compareTag = solver.compareTo(o.solver);

		double time_one = Double.parseDouble(time.toString());
		double time_two = Double.parseDouble(o.getValue().toString());
		
	
		if(compareTag == 0) {
        	if(time_one > time_two)
        		return 1;
        	else if(time_one == time_two)
        		return 0;
        	else return -1;
		}
		
		else return compareTag;
	}

}