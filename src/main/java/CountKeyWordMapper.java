import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by George on 3/21/2016.
 */
public class CountKeyWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        Text keyWordText = new Text();
        IntWritable countWritable = new IntWritable();
        private KeywordParser parser = new KeywordParser();
        enum KeyWordError {
            NO_KEYWORD_FOUND
        }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
                parser.parseKeyWord(value);
                if (parser.foundKeyWord()) {
                    String keyWord = parser.getKeyWord();
                    keyWordText.clear();
                    keyWordText.set(keyWord);
                    countWritable.set(1);
                    context.write(keyWordText, countWritable);
                } else {
                    System.err.println("Ignoring possibly corrupt input: " + value);
                    context.getCounter(KeyWordError.NO_KEYWORD_FOUND).increment(1);
                }

        }catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}