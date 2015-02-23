
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;

/**
 * This class read "Year Star" and a list of IntWritables, and emits the count
 * of each key
 */
public class Mov_NotHasRate_Reducer extends MapReduceBase
        implements Reducer<WritableComparable, Writable, WritableComparable, Writable> {

        public void reduce(WritableComparable key, Iterator values,
                OutputCollector output, Reporter reporter) throws IOException {

                int[] flag = {0, 0, 0, 0, 0};//debug

                while (values.hasNext()) {
                        flag[((IntWritable) values.next()).get() - 1] = 1;
                }
                if (flag[0] == 0) {
                        output.collect((Text) key, new IntWritable(1));
                }
                if (flag[4] == 0) {
                        output.collect((Text) key, new IntWritable(5));
                }
        }
}
/*
 public class MovHasrateReducer extends MapReduceBase
 implements Reducer<Text, IntWritable, Text, IntWritable> {

 public void reduce(Text key, Iterator<IntWritable> values,
 OutputCollector output, Reporter reporter) throws IOException {
                
 int[] flag = {0, 0, 0, 0, 0};

 while (values.hasNext()) {
 flag[values.next().get()-1] = 1;
 }
 for (int i = 0;i<5;++i){
 output.collect(key, new IntWritable(flag[i]));
 }
 }
 }


 public void reduce233(WritableComparable key, Iterator values,
 OutputCollector output, Reporter reporter) throws IOException {

 //output.collect((Text) key, new IntWritable(999));
 while (values.hasNext()) {
 int rate = ((IntWritable) values.next()).get();
 output.collect((Text) key, new IntWritable(rate -));
 }
 }
 */
