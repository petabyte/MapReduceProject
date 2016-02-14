package solution;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxWordLength2Reducer extends Reducer<Text, Text, Text, Text> {

	Text textOutput = new Text();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
    /*
     * Solution
     */
		int maxValue = 0;
		List<String> sampleWords = new ArrayList<String>();
		StringBuilder stringBuilder = new StringBuilder();
		for(Text value : values) {
			String currentStringValue = value.toString();
			int currentStringValueSize = currentStringValue.length();
			if(currentStringValueSize > maxValue){
				sampleWords.clear();
				sampleWords.add(currentStringValue);
				maxValue = currentStringValueSize;
			}else{
				if( currentStringValueSize == maxValue) {
					if(!sampleWords.contains(currentStringValue)){
						sampleWords.add(currentStringValue);
					}
				}
			}
		}
		stringBuilder.append(Integer.toString(maxValue));
		for(String sampleWord: sampleWords){
			stringBuilder.append(","+sampleWord);
		}
		textOutput.set(stringBuilder.toString());
		context.write(key, textOutput);
	}
}