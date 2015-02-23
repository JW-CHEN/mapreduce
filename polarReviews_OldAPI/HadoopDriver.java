
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/**
 * Driver class for PolyHadoop project. Runs the Hadoop instance, setting up the
 * Map/Reduce classes, input/output paths, and the Key/Value classes.
 *
 * This does not use a Combiner class - it would add complexity to reduce
 * (necessitating a weighted average), for little to no gain. In the common
 * case, a user will only rate a movie once, and each MapTask probably operates
 * on a single data file at a time.
 *
 * Invoke with 2 arguments, or 1.
 *
 * @author Daniel
 *
 */
public class HadoopDriver {

        public static void main(String[] args) {
                /* Require args to contain the paths */
                if (args.length != 2) {
                        System.err.println("Error! Usage: \n"
                                + "HadoopDriver <input dir> <output dir>\n");
                        System.exit(1);
                }

                //JobClient client = new JobClient();
                Path tempPath = new Path("temp");

                //===============configuration 1
                JobConf conf = new JobConf(HadoopDriver.class);

                /* DayRatingMapper outputs (Text, IntWritable) */
                conf.setMapOutputKeyClass(Text.class);
                conf.setMapOutputValueClass(IntWritable.class);

                /* RatingPerDayReducer outputs (Text, IntWritable) */
                conf.setOutputKeyClass(WritableComparable.class);
                conf.setOutputValueClass(Writable.class);

                /* Pull input and output Paths from the args */
                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(args[0]));
                FileOutputFormat.setOutputPath(conf, tempPath);

                /* Set to use Mapper and Reducer classes 
                 Do not use Combiner first*/
                conf.setMapperClass(MovRateMapper.class);
                //conf.setCombinerClass(MovHasrateReducer.class);
                conf.setReducerClass(Mov_NotHasRate_Reducer.class);

                conf.set("mapred.child.java.opts", "-Xmx2048m");

                //===================configuration 2
                JobConf conf2 = new JobConf(HadoopDriver.class);

                /* DayRatingMapper outputs (Text, IntWritable) */
                conf2.setMapOutputKeyClass(Text.class);
                conf2.setMapOutputValueClass(IntWritable.class);

                /* RatingPerDayReducer outputs (Text, IntWritable) */
                conf2.setOutputKeyClass(WritableComparable.class);
                conf2.setOutputValueClass(Writable.class);

                /* Pull input and output Paths from the args */
                conf2.setInputFormat(TextInputFormat.class);
                conf2.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf2, tempPath);
                FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

                /* Set to use Mapper and Reducer classes 
                 Do not use Combiner first*/
                conf2.setMapperClass(notHasRate_mov_Mapper.class);
                //conf.setCombinerClass(MovHasrateReducer.class);
                conf2.setReducerClass(notHasRate_numMov_Reducer.class);

                try {
                        //client.setConf(conf);
                        JobClient.runJob(conf);

                        //client.setConf(conf2);
                        JobClient.runJob(conf2);

                } catch (Exception e) {
                        e.printStackTrace();
                }
        }

}
