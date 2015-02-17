import java.io.IOException;                                                             
import java.util.Iterator;                                                              
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;                                                
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class FnameRateDupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),\\d{4}-\\d{2}-\\d{2}$");
	public static Pattern movieID = Pattern.compile("^(\\w+)_(\\d+).(\\w+)$");

	private String getMovie(String path) throws IOException {
		String line;
		
		InputStream fis = new FileInputStream(path);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		line = br.readLine();

		return line;
	}

	public void map(LongWritable key, Text values, Context context) throws InterruptedException, IOException {
	
		String line = values.toString();
		Matcher userRate = userRatingDate.matcher(line);
		Text movName = new Text();
		IntWritable rateDup = new IntWritable(); 
	
		/*
 		String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
		String movie = getMovie(fileName);
		*/
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		Matcher movNameFromFile = movieID.matcher(fileName);
		
		if (movNameFromFile.matches())
			movName.set(movNameFromFile.group(2));
		
		if (userRate.matches()) {
			rateDup.set(Integer.parseInt(userRate.group(2)));
			context.write(movName, rateDup);
		} else {
			System.out.println("not match");
		}
	}		
}
