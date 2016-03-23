import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by George on 3/22/2016.
 */
public class TopKeyWordMapper extends Mapper<LongWritable, Text, Text, KeyWordCustomMapperWritable> {

     Text keyWordText = new Text();
     KeyWordCustomMapperWritable keyWordMapperWritable = new KeyWordCustomMapperWritable();
     private KeywordParser parser = new KeywordParser();
     DateFormat df = new SimpleDateFormat("yyyyMMdd");
     Calendar calendar = Calendar.getInstance();

    enum KeyWordError {
        NO_KEYWORD_FOUND
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        try {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] fileNameSplit = filename.split("_");
            String[] fileNameSplit2 = fileNameSplit[2].split("\\.");
            String fileDateString = fileNameSplit2[0];
            Date fileDate = df.parse(fileDateString);
            calendar.setTime(fileDate);
            int month = calendar.get(Calendar.MONTH);

            parser.parseKeyWord(value);
            if (parser.foundKeyWord()) {
                String keyWord = parser.getKeyWord();
                keyWordText.clear();
                keyWordText.set(keyWord);
                keyWordMapperWritable.setCount(1);
                keyWordMapperWritable.setMonth(month);
                context.write(keyWordText, keyWordMapperWritable);
            } else {
                System.err.println("Ignoring possibly corrupt input: " + value);
                context.getCounter(KeyWordError.NO_KEYWORD_FOUND).increment(1);
            }

        }catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        }catch(ParseException pe){
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}
