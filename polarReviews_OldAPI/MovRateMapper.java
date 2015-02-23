
import java.io.IOException;
//import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//for file name
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobConf;

public class MovRateMapper extends MapReduceBase
        implements Mapper<WritableComparable, Text, Text, IntWritable> {

        private static Pattern patUserRateDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2})$");
        private static Pattern patMov = Pattern.compile("^.*mv_(\\d+).txt$");

        private String inputFile;

        public void configure(JobConf job) {

                //mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
                String inputPath = job.get(JobContext.MAP_INPUT_FILE);

                Matcher matName = patMov.matcher(inputPath);
                if (matName.matches()) {
                        inputFile = matName.group(1);
                } else {
                        System.out.println("File Name Not Match!");
                }
        }

        public void map(WritableComparable key, Text values, OutputCollector output, Reporter reporter)
                throws IOException {

                String line = values.toString();
                Matcher matUserRateDate = patUserRateDate.matcher(line);

                if (line.matches("^\\d+:$")) {
                        /* This is just Moive ID line, Ignore it */
                } else if (matUserRateDate.matches()) {
                        String rate = matUserRateDate.group(2);
                        output.collect(new Text(inputFile), new IntWritable(Integer.parseInt(rate)));
                } else {
                        /* should not occur. The input is in an invalid format */
                        System.err.println("no match");
                }
        }

}
