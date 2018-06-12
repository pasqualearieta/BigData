import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/* Word Count with new Interface */
public class BigDataTest {

	
	//THIS MAPPER WRITE OUT ONLY THE COMPLETE INSTANCE OF A CERTAIN SOLVER. ONLY TIME AND SOLVER NAME
	//ARE OUTPUTED
	/* Mapper */
	static class MapperSolver extends Mapper<LongWritable, Text, CustomWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, CustomWritable, Text>.Context context)
				throws IOException, InterruptedException {

			if (!key.equals(0)) {
				String[] values = value.toString().split("\t");

				String solver = values[0];

				if (values[14].equals("solved")) {
					CustomWritable w = CustomWritable.read(new Text(solver.toString()),
							new Text(values[11].toString()));
					context.write(w, new Text(values[11].toString()));
				}
			}
		}
	}

	//THIS REDUCER, FOR EACH SOLVER WRITE AS KEY THE NAME OF THE SOLVER, WHILE AS VALUE ALL THE TIME OF
	//COMPLETE INSTANCE
	/* Reducer */
	static class ReducerSolver extends Reducer<CustomWritable, Text, Text, Text> {
		@Override
		protected void reduce(CustomWritable arg0, Iterable<Text> arg1,
				Reducer<CustomWritable, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {

			String times = "";

			for (Text t : arg1)
				times += t.toString() + "\t";

			arg2.write(new Text(arg0.getSolver()), new Text(times.toString()));
		}

	}

	//THIS SECOND MAPPER, PUT AN ID-NUMBER FOR EACH SPLITTED VALUES, IN PARTICULAR THE KEY IS AN ID.NUMBER
	//WHILE THE VALUE ARE COMPOSED BY SOLVER-NAME#TIME_REQUIRED_FOR_THE_I_(ESIMA)_EXECUTION#ID
	static class MapperTable extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] values = value.toString().split("\t");
			String solver = values[0];
			context.write(new LongWritable(0), new Text(solver));
			for (int i = 1; i < values.length; i++)
				context.write(new LongWritable(i), new Text(solver + "#" + values[i] + "#" + i + "\t"));
		}
	}

	//THE OUTPUT OF THIS REDUCER IS THE REQUIRED TABLE, IN PARTICULAR, IT HANDLE ROW OF DIFFERENT TYPE:
	//THE FIRST ONE WITH KEY 0, CONTAIN ALL THE SOLVER NAME
	//FROM THE SECOND TO THE LAST, ARE PRESENT FOR THE I_(ESIMA) INSTANCE THE SOLVER NAME AND THE TIME OF EACH SOLVER.
	//IN THIS WAY, BY CREATING A MAP, CONTAINING AS KEY THE SOLVER AND AS VALUE THE CURRENT TIME (CHANGED ITERATION BY ITERATION)
	//IT IS POSSIBLE TO REPRESENT THE DATA IN A TABULAR WAY. 
	static class ReducerTable extends Reducer<LongWritable, Text, Text, Text> {

		Map<String, String> temporary_storage = new HashMap<String, String>();
		boolean first = true;
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,
				Reducer<LongWritable, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {

			
			String convert="";
			for(Text t : arg1)
				convert+=t.toString();
			
			String[] values = convert.toString().split("\t");
			if (first) {
				String header = "";
				
				for (int i = 0; i < values.length; i++)
					temporary_storage.put(values[i], "");
				Set<String> key_set = temporary_storage.keySet();

				Iterator<String> it = key_set.iterator();
				while (it.hasNext()) {
					header += it.next() + "\t";
				}
				arg2.write(new Text("Nr.Instances"), new Text(header));
				first = false;
			} 
			
			else {
				for(int i = 0; i < values.length; i++) {
					String current = values[i];
					String[] t = current.split("#");
					temporary_storage.put(t[0], t[1]);
				}
				
				String times = "";
				
				for (Map.Entry<String, String> entry : temporary_storage.entrySet()) {
					String key = entry.getKey();
				    String value = entry.getValue();
				    times+=value+"\t";
				    
				    temporary_storage.put(key, "");
				}
				 
				arg2.write(new Text(arg0 + ""), new Text(times.toString()+""));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path inputTemp = new Path(files[1]);
		Path outputFinal = new Path(files[2]);

		Job job = Job.getInstance(conf, "JOB_ORDER");
		job.setJarByClass(BigDataTest.class);
		job.setMapperClass(MapperSolver.class);
		job.setMapOutputKeyClass(CustomWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReducerSolver.class);
		job.setSortComparatorClass(SolverComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, inputTemp);

		boolean success = job.waitForCompletion(true);

		if (success) {
			Job jobTable = Job.getInstance(conf, "JOB_TABLE");

			jobTable.setMapperClass(MapperTable.class);
			jobTable.setReducerClass(ReducerTable.class);

			jobTable.setMapOutputKeyClass(LongWritable.class);
			jobTable.setMapOutputValueClass(Text.class);

			jobTable.setOutputKeyClass(Text.class);
			jobTable.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobTable, inputTemp);
			FileOutputFormat.setOutputPath(jobTable, outputFinal);

			success = jobTable.waitForCompletion(true);
		}

		if (success)
			System.exit(1);
		else
			System.exit(0);
	}

}