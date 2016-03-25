import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by George on 3/24/2016.
 */
public class SortTopWordTaxSeasonMapper extends Mapper<Text, Text, IntWritable, Text> {

    static enum BAD_COUNT {
        BAD_TOTAL_COUNT
    };

    IntWritable keyIntWritable = new IntWritable();
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String lineRecord = value.toString();
            String [] lineSplit = lineRecord.split("\\t+");
            String firstElement = lineSplit[1];
            keyIntWritable.set(Integer.parseInt(firstElement));
            context.write(keyIntWritable, key);
        }catch(NumberFormatException nfe){
            context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
        }catch(ArrayIndexOutOfBoundsException aie){
            context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
        }
    }
}
