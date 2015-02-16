import java.io.IOException;                                                             
import java.util.Iterator;                                                              
import java.util.regex.Matcher;                                                         
import java.util.regex.Pattern;                                                         
                                                                                        
import org.apache.hadoop.io.IntWritable;                                                
import org.apache.hadoop.io.Text;                                                       
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;                                          
import org.apache.hadoop.mapred.Mapper;                                                 
import org.apache.hadoop.mapred.OutputCollector;                                        
import org.apache.hadoop.mapred.Reducer;                                                
import org.apache.hadoop.mapred.Reporter;  

public class NumRevUserMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {

	//private static Pattern userNumReview = Pattern.compile("^(\\d+),(\\d+)");
	// user original tab seperator
	private static Pattern userNumReview = Pattern.compile("^(\\d+)\\t(\\d+)");
	
	/**
 	Give the input as : UserID, numReviews
	Emit (numReviews, UserID)
	**/

	public void map(LongWritable key, Text values,
			OutputCollector output, Reporter reporter) throws IOException {
		
		String line = values.toString();
		Matcher userRev = userNumReview.matcher(line);
		Text numReview = new Text();
		Text userID = new Text();
		
		if (userRev.matches()) {
			numReview.set(userRev.group(2));
			userID.set(userRev.group(1));
			output.collect(numReview, userID);
		} else {
			// should not occur
			System.out.println("not match");
		}
	}
}		
