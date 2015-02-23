
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class DateNumrateReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> value, Context context)
                throws IOException, InterruptedException {

                int sum = 0;

                while (value.hasNext()) {
                        sum += value.next().get();
                        //db
                        sum*=10;
                }
                System.out.println("haha");

                context.write(key, new IntWritable(sum));
        }
}
