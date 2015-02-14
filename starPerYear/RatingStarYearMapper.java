import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/*
 *  This Mapper emit: ("Year Star", Count)
 *  Find the star per year in one file
 */

public class RatingStarYearMapper extends MapReduceBase
implements Mapper<WritableComparable, Writable, WritableComparable, Writable> {
	
	private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4})-\\d{2}-\\d{2}$");
	private Text yearStar = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	public void map(WritableComparable key, Writable values,
			OutputCollector output, Reporter reporter) throws IOException {
		
		/* key is the offset of text in the file*/
		/* values is the content of line in file*/
		String line = ((Text)values).toString();
		Matcher userRating = userRatingDate.matcher(line);
		
		String strYearStar;
		
		if (line.matches("^\\d+:$")) {
			/* This is just Moive ID line, Ignore it */	
		} else if (userRating.matches()) {
			strYearStar = userRating.group(3) + " " + userRating.group(2);
			yearStar.set(strYearStar);
			output.collect(yearStar, one);
		} else {
			/* should not occur. The input is in an invalid format */
		}
		
	}
}
			
		
