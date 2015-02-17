
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


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

	public static void main(String[] args) {
		if (args.length != 1 && args.length != 2) {
			System.err.println("Error! Usage: \n" +
					"HadoopDriver <input dir> <output dir>\n" + 
					"HadoopDriver <job.xml>");
			System.exit(1);
		}
		
		JobClient client = new JobClient();
		JobConf conf1 = null;
		JobConf conf2 = null;

		if (args.length == 2) {
			conf1 = new JobConf(HadoopDriver.class);
			
			conf1.setMapOutputKeyClass(Text.class);
			conf1.setMapOutputValueClass(IntWritable.class);
			
			conf1.setOutputKeyClass(Text.class);
			conf1.setOutputValueClass(Text.class);
			
			conf1.setInputFormat(TextInputFormat.class);
			conf1.setOutputFormat(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf1, new Path(args[0]));
			Path tempPath = new Path("temp");
			FileOutputFormat.setOutputPath(conf1, tempPath);

			conf1.setMapperClass(UserOneReviewMapper.class);
			conf1.setReducerClass(UserNumReviewReducer.class);
			conf1.set("mapred.child.java.opts", "-Xmx2048m");
			
			// conf2 for job 2
			conf2 = new JobConf(HadoopDriver.class);
			
			conf2.setMapOutputKeyClass(Text.class);
			conf2.setMapOutputValueClass(Text.class);
			
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(IntWritable.class);

			FileInputFormat.setInputPaths(conf2, tempPath);
			FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

			conf2.setMapperClass(NumRevUserMapper.class);
			conf2.setReducerClass(NumRevNumUserReducer.class);

			conf2.set("mapred.child.java.opts", "-Xmx2048m");
			
		}

		client.setConf(conf1);
		try {
			JobClient.runJob(conf1);
		} catch (Exception e) {
			e.printStackTrace();
		}
		client.setConf(conf2);
		try {
			JobClient.runJob(conf2);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

