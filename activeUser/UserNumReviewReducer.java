import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** 
 *  This class read a list (userID, one), and calc userID's total #review
 *  The output format should be formatted as (Text, Text)
 */

public class UserNumReviewReducer extends MapReduceBase
implements Reducer<Text, IntWritable, Text, Text>
{
	
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		int sum = 0;
		//IntWritable numReview = new IntWritable();	
		Text numReview = new Text();

		while (values.hasNext()) {
			sum += values.next().get();
		}
		
		numReview.set(Integer.toString(sum));
		//numReview.set(sum);
		output.collect(key, numReview);
	}
}
