
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** 
 *  This class read "Year Star" and a list of IntWritables, and emits the count of each key
 */

public class StarPerYearReducer extends MapReduceBase
implements Reducer<WritableComparable, Writable, WritableComparable, Writable>
{
	
	public void reduce(WritableComparable key, Iterator values,
			OutputCollector output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += ((IntWritable)values.next()).get();
		}
		output.collect(key, new IntWritable(sum));
	}
}
