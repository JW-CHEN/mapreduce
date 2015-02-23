
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HadoopDriver {

        public static void main(String[] args) throws Exception {
                if (args.length != 1 && args.length != 2) {
                        System.err.println("Error! Usage: \n"
                                + "HadoopDriver <input dir> <output dir>\n"
                                + "HadoopDriver <job.xml>");
                        System.exit(1);
                }

                Configuration conf = new Configuration();
                //Job job = new Job(conf, "job1");
                Job job = Job.getInstance(conf, "job3");//new
                if (args.length == 2) {
                        job.setJarByClass(HadoopDriver.class);

                        job.setMapOutputKeyClass(Text.class);
                        job.setMapOutputValueClass(IntWritable.class);

                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(IntWritable.class);

                        job.setInputFormatClass(TextInputFormat.class);
                        job.setOutputFormatClass(TextOutputFormat.class);

                        job.setMapperClass(DateOneMapper.class);
                        job.setCombinerClass(DateNumrateReducer.class);
                        job.setReducerClass(DateNumrateReducer.class);

                        job.setNumReduceTasks(1);

                        FileInputFormat.addInputPath(job, new Path(args[0]));
                        FileOutputFormat.setOutputPath(job, new Path(args[1]));
                }

                job.waitForCompletion(true);

        }
}
