import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
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
    FileSystem hdfs = null;
    public static Logger log = Logger.getLogger(AnalysisKeyWordMapper.class);
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI mappingFileUri = context.getCacheFiles()[0];
            if (mappingFileUri != null) {
                try {
                    hdfs = FileSystem.get(context.getConfiguration());
                    // Would probably be a good idea to inspect the URI to see what the bit after the # is, as that's the file name
                    log.info("Reading file: " +mappingFileUri);
                    String fileLine = IOUtils.toString(new InputStreamReader(hdfs.open(new Path(mappingFileUri)), Charsets.UTF_8));
                    log.info("This is what is in the file: " +fileLine);
                    String[] fileLineSplit = fileLine.split("\\t+");
                    log.info("Parsing this value: ["+ fileLineSplit[1]+"]");
                    String intValue = fileLineSplit[1].trim().replaceAll("\r","").replaceAll("\n","");
                    log.info("int Value: ["+ intValue+"]");
                    totalCount = Integer.parseInt(intValue);
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
            String [] lineSplit = lineRecord.split("\\t+");
            String firstElement = lineSplit[0].trim();
            String secondElement = lineSplit[1].trim();
            double totalKeyword = Double.parseDouble(firstElement);
            double totalKeywordForTaxSeason = Double.parseDouble(secondElement);
            double totalOtherKeywords = totalCount - totalKeyword;
//            log.info("Keyword: ["+ key.toString()+"]");
//            log.info("Total for Keyword: ["+ totalKeyword+"]");
//            log.info("Total for Keyword Tax Season: ["+ totalKeywordForTaxSeason+"]");
//            log.info("Total for Other Keywords: ["+ totalOtherKeywords+"]");
//            log.info("Total : ["+ totalCount+"]");
            double chi1 = totalKeywordForTaxSeason / totalCount ;
            double chi2 = totalKeyword / totalCount ;
            double expected = chi1 * chi2 * totalCount;


            //log.info("chi Divisor : ["+ chiDivisor  +"]");
            double chiDouble =  (Math.pow((totalKeywordForTaxSeason - expected), 2) / expected);
            double chiValue = !Double.isNaN(chiDouble) ? chiDouble : 0.0;
           // log.info("chi Value : ["+ chiValue  +"]");

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
