import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by George on 3/21/2016.
 */
public class CountKeyWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    Text totalAllKeywWords = new Text("Total");
    IntWritable keyWordsCountIntWritable = new IntWritable();
    int keyWordsCount = 0;
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        for (IntWritable value : values) {
            keyWordsCount += value.get();
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        keyWordsCountIntWritable.set(keyWordsCount);
        context.write(totalAllKeywWords, keyWordsCountIntWritable);
    }
}
