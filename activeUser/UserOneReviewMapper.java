import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UserOneReviewMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4}-\\d{2}-\\d{2})$");

	/*
 	 read a line of input from each file: UserID, RatingValue, RatingDate
 	 emit(UserID, one)
	*/
	
	public void map(LongWritable key, Text values,
			OutputCollector output, Reporter reporter) throws IOException {
		
		String line = values.toString();
		Matcher userRating = userRatingDate.matcher(line);
		
		Text userID = new Text();

		if (line.matches("^\\d+:$")) {
			// this is movie ID, ignore it
		} else if (userRating.matches()) {
			userID.set(userRating.group(1));
			output.collect(userID, one);
		} else {
			// should no occur
		}
	}
}
		
