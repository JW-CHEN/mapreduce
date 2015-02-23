import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

public class DateOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static Pattern patUserRateDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2})$");
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text values, Context context)
                throws InterruptedException, IOException {

                String line = values.toString();
                Matcher matUserRateDate = patUserRateDate.matcher(line);
                Text data = new Text();

                if (line.matches("^\\d+:$")) {
                        // This is the Movie ID line. Ignore it
                } else if (matUserRateDate.matches()) {
                        data.set(matUserRateDate.group(3));
                        context.write(data, one);
                } else {
                        System.out.println("not match");
                }
        }
}
