import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by George on 3/24/2016.
 */
public class AnalysisKeyWordReducer extends Reducer<DoubleWritable, Text,DoubleWritable, Text > {

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text value :values){
            context.write(key, value);
        }
    }
}
