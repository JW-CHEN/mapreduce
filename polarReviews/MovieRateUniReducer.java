import java.io.IOException;                                                             
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
                
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class MovieRateUniReducer extends Reducer<Text, IntWritable, Text, Text>
{
	
	private	int[] flag = {0,0,0,0,0};
	
	public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
		while (values.hasNext())
			flag[values.next().get()-1] = 1;
		for (int i = 0; i < 5; i++) {
			if (flag[i] == 0)
				context.write(key, new Text(Integer.toString(i+1)));
		}	
	}
}
