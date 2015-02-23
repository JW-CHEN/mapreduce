import java.io.IOException;                                                             
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
                
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class MovieRateUniReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	
	public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
		int[] flag = {0,0,0,0,0};
		
		int count = 0;
		while (values.hasNext()){
			count += values.next().get();
			context.write(new Text("debug"), new IntWritable(count));
		}
		context.write(key, new IntWritable(99999));
		
		
		/*
		
		while (values.hasNext()){
			flag[values.next().get()-1] = 1;
		}
		for (int i = 0; i < 5; i++) {
			if (flag[i] == 0)
				context.write(key, new Text(Integer.toString(i+1)));
		}
		*/
	}
}
