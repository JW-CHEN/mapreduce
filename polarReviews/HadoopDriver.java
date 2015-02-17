
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Drive class for Active User calculation. 
 * Run Two Consecutive Map--Reduce Jobs: mapper1, reducer1, mapper2, reducer2
 * Path: 
 * mapper1 read from "input" path
 * reducer1 write to "temp" path
 * mapper2 read from "temp" path
 * reducer2 write to "output" path
 *
 * User Two JobClient to run
 */

public class HadoopDriver {

	public static void main(String[] args) throws Exception{
		if (args.length != 1 && args.length != 2) {
			System.err.println("Error! Usage: \n" +
					"HadoopDriver <input dir> <output dir>\n" + 
					"HadoopDriver <job.xml>");
			System.exit(1);
		}
		
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		Job job = new Job(conf1, "job1");
		job.setJarByClass(HadoopDriver.class);
		/*Job job2 = new Job(conf2, "job2");
		job2.setJarByClass(HadoopDriver.class);
		*/
		if (args.length == 2) {
			// If the Mapper has the different class with Reducer must have following two class setting
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);		
	
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapperClass(FnameRateDupMapper.class);
			job.setReducerClass(MovieRateUniReducer.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			Path tempPath = new Path("temp");
			FileOutputFormat.setOutputPath(job, tempPath);

			// conf2 for job 2
				
			/*job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			job2.setMapperClass(NoXRateMovieMapper.class);
			job2.setReducerClass(NoXRateNumMovReducer.class);

			FileInputFormat.addInputPath(job2, tempPath);
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			*/
		}

		job.waitForCompletion(true);
		//job2.waitForCompletion(true);
	}
}

