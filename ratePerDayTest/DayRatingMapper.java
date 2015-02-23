
import java.io.IOException;
//import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

//for file name
import org.apache.hadoop.mapred.JobContext;

public class DayRatingMapper extends MapReduceBase
        implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {

        private final static IntWritable one = new IntWritable(1);
        private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2})$");

        private String inputFile;

        public void configure(JobConf job) {
                //mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
                inputFile = job.get(JobContext.MAP_INPUT_FILE);
        }

        /**
         * Given a line of input as : UserID, RatingValue, RatingDate line, it
         * extracts the RatingDate, and emits(RatingDate, 1)
         *
         */
        public void map(WritableComparable key, Writable values,
                OutputCollector output, Reporter reporter) throws IOException {

                String line = ((Text) values).toString();
                // parse every line read from file
                Matcher userRating = userRatingDate.matcher(line);

                // output date from line
                Text date = new Text();

                if (line.matches("^\\d+:$")) {
                        // This is the Movie ID line. Ignore it
                } else if (userRating.matches()) {
                        date.set(userRating.group(3));

                        //=================
                        date.set(inputFile);
                        //==================
                        output.collect(date, one);
                } else {
                        // should not occur. The input is in an invalid format.
                }
        }
}
