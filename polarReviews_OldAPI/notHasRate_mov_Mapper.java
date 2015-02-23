
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

public class notHasRate_mov_Mapper extends MapReduceBase
        implements Mapper<WritableComparable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        private static Pattern pat_mov_notHasRate = Pattern.compile("^\\d+\t(\\d)$");


        public void map(WritableComparable key, Text values, OutputCollector output, Reporter reporter)
                throws IOException {

                String line = values.toString();
                Matcher mat = pat_mov_notHasRate.matcher(line);

                if (mat.matches()) {
                        String rate = mat.group(1);
                        output.collect(new Text(rate), one);
                } else {
                        /* should not occur. The input is in an invalid format */
                        System.err.println("no match");
                }
        }

}
