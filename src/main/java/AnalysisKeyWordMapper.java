import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by George on 3/24/2016.
 */
public class AnalysisKeyWordMapper  extends Mapper<Text, Text, DoubleWritable, Text> {

    static enum BAD_COUNT {
        BAD_TOTAL_COUNT
    };
    Integer totalCount = 0;
    DoubleWritable analysisDoubleWritable = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI mappingFileUri = context.getCacheFiles()[0];

            if (mappingFileUri != null) {
                try {
                // Would probably be a good idea to inspect the URI to see what the bit after the # is, as that's the file name
                File fileToRead = new File(mappingFileUri.getPath());
                String fileLine = FileUtils.readFileToString(fileToRead);
                String[] fileLineSplit = fileLine.split("\\s+");
                totalCount = Integer.parseInt(fileLineSplit[1]);
                }catch(NumberFormatException nfe){
                    context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
                }catch(ArrayIndexOutOfBoundsException aie){
                    context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
                }
            }
        }
    }

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String lineRecord = value.toString();
            String [] lineSplit = lineRecord.split("\\s+");
            String firstElement = lineSplit[0];
            String secondElement = lineSplit[1];
            int totalKeyword = Integer.parseInt(firstElement);
            int totalKeywordForTaxSeason = Integer.parseInt(secondElement);
            int totalOtherKeywords = totalCount - totalKeyword;
            double chiValue = totalKeywordForTaxSeason/((totalKeyword * totalOtherKeywords)/totalCount);
            analysisDoubleWritable.set(chiValue);
            context.write(analysisDoubleWritable,key);
        }catch(NumberFormatException nfe){
            context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
        }catch(ArrayIndexOutOfBoundsException aie){
            context.getCounter(BAD_COUNT.BAD_TOTAL_COUNT).increment(1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
