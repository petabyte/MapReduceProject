package solution;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxWordLength1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  IntWritable intWritable = new IntWritable();
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    /*
     * Solution
     */
	  int maxValue = 0;
	  for(IntWritable value : values) {
		    int currentValue = value.get();
		    if( currentValue > maxValue) {
		        maxValue = currentValue;
		    }
		}
	  intWritable.set(maxValue);
	  context.write(key, intWritable);
  }
}