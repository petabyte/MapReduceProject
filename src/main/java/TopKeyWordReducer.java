import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Calendar;

/**
 * Created by George on 3/22/2016.
 */
public class TopKeyWordReducer extends Reducer<Text, KeyWordCustomMapperWritable, Text, KeyWordCustomReduceWritable> {

    KeyWordCustomReduceWritable keyWordCustomReduceWritable = new KeyWordCustomReduceWritable();

    @Override
    public void reduce(Text key, Iterable<KeyWordCustomMapperWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        int keyWordsCount = 0;
        int keyWordCountTaxSeason = 0;
        for (KeyWordCustomMapperWritable value : values) {
             int count = value.getCount();
             int month = value.getMonth();
             keyWordsCount += count;
             if(month == Calendar.MARCH || month == Calendar.APRIL){
                keyWordCountTaxSeason += count;
             }
        }
        keyWordCustomReduceWritable.setCountTotal(keyWordsCount);
        keyWordCustomReduceWritable.setCountTaxSeason(keyWordCountTaxSeason);
        context.write(key, keyWordCustomReduceWritable);
    }
}
