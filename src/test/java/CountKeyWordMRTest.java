import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 3/22/2016.
 */
public class CountKeyWordMRTest  {  /*
    * Declare harnesses that let you test a mapper, a reducer, and
    * a mapper and a reducer working together.
    */
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        CountKeyWordMapper mapper = new CountKeyWordMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        CountKeyWordReducer reducer = new CountKeyWordReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper() throws IOException {
        Path mockPath = new Path("CP_UserSearch_20160306.zip.queries.log");
        mapDriver.setMapInputPath(mockPath);
        mapDriver.withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"     1testKeyword1    \"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"\"   giftTax   \"\"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\tdog and cat\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("badData"));
        mapDriver.withOutput(new Text("1testkeyword1"), new IntWritable(1));
        mapDriver.withOutput(new Text("\"1testkeyword1\""), new IntWritable(1));
        mapDriver.withOutput(new Text("\"gifttax\""), new IntWritable(1));
        mapDriver.withOutput(new Text("dog and cat"), new IntWritable(1));
        mapDriver.runTest();
    }

    /*
  * Test the reducer.
  */
    @Test
    public void testReducer() throws IOException {

        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("charitable contribution"), values);
        reduceDriver.withOutput(new Text("Total"), new IntWritable(4));
        reduceDriver.runTest();
    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"     1testKeyword1    \"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"\"   giftTax   \"\"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\tdog and cat\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("badData"));
        mapReduceDriver.withOutput(new Text("Total"), new IntWritable(4));
        mapReduceDriver.runTest();

    }

}

