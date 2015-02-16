import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *  This class read a list of Text, Text, and emit the total num of users for certain rating
 */

public class NumRevNumUserReducer extends MapReduceBase
implements Reducer<Text, Text, Text, IntWritable>
{
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	
		int sum = 0;
	
		while (values.hasNext()) {
			sum += 1;
			values.next();
		}
		
		output.collect(key, new IntWritable(sum));
	}
}
	
