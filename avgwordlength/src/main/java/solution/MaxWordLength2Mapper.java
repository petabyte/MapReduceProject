package solution;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxWordLength2Mapper extends Mapper<LongWritable, Text, Text, Text> {

  Text valueText = new Text();
  Text keyText = new Text();
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
				  keyText.set(firstLetter);
				  valueText.set(word);
				  context.write(keyText, valueText);
			  }			  
			}
		}
  }
}
