import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by George on 3/24/2016.
 */
public class AnalysisKeyWordDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf("Usage: AnalysisKeyWordDriver <total file> <input dir> <output dir>\n");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(AnalysisKeyWordDriver.class);
        job.setJobName("AnalysisKeyWordDriver");
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.addCacheFile(filePath.toUri());
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(AnalysisKeyWordMapper.class);
        job.setReducerClass(AnalysisKeyWordReducer.class);
        job.setSortComparatorClass(DoubleComparator.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /*
     * The main method calls the ToolRunner.run method, which
     * calls an options parser that interprets Hadoop command-line
     * options and puts them into a Configuration object.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AnalysisKeyWordDriver(), args);
        System.exit(exitCode);
    }
}

