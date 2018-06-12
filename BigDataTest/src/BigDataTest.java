import java.io.IOException;
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

	/* Mapper */
	static class MapperSolver extends Mapper<LongWritable, Text, CustomWritable, Text >{	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,  CustomWritable, Text>.Context context)
				throws IOException, InterruptedException {
	
			if(!key.equals(0)) {
			String[] values = value.toString().split("\t");
			
			String solver = values[0];
			
			if(values[14].equals("solved")) {
				CustomWritable w = CustomWritable.read(new Text(solver.toString()), new Text(values[11].toString()));
				context.write(w, new Text(values[11].toString()));
			}
		}
	}
}
	
	/* Reducer */
	static class ReducerSolver extends Reducer<CustomWritable, Text, Text, Text>
	{
		@Override
		protected void reduce(CustomWritable arg0, Iterable<Text> arg1, Reducer<CustomWritable, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
				
				String times = ""; 
				
				for(Text t: arg1)
					times+=t.toString()+"\t";
				
				arg2.write(new Text(arg0.getSolver()), new Text(times.toString()));
		}
		
	}

	static class MapperTable extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count=0;
			String[] values = value.toString().split("\t");	
			for(String s: values)
			context.write(new Text(count), new Text(s+count+"");
		}
	}
	
	static class ReducerTable extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
		
			
				
				for(Text t : arg1) {
					
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

		if(success) {
			Job jobMean = Job.getInstance(conf, "JOB_TABLE");
			
			jobMean.setMapperClass(MapperTable.class);
			jobMean.setReducerClass(ReducerTable.class);
			
			jobMean.setMapOutputKeyClass(Text.class);
			jobMean.setMapOutputValueClass(Text.class);

			jobMean.setOutputKeyClass(Text.class);
			jobMean.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(jobMean, inputTemp);
			FileOutputFormat.setOutputPath(jobMean, outputFinal);
			
			success = jobMean.waitForCompletion(true);
		}
	
		
		
		if (success)
			System.exit(1);
		else
			System.exit(0);
	}

}