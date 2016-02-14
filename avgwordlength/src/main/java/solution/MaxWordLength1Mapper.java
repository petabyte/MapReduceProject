package solution;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxWordLength1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  IntWritable intWritable = new IntWritable();
  Text text = new Text();
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    /*
     * Solution
     */
	  String line = value.toString();
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
			  String firstLetter = word.substring(0,1);
			  if(firstLetter.matches("[a-z]")){
				  text.set(firstLetter);
				  intWritable.set(word.length());
				  context.write(text, intWritable);
			  }			  
			}
		}
  }
}
