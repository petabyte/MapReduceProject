import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.internal.mapred.MockReporter;
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
public class TopKeyWordMRTest {  /*
    * Declare harnesses that let you test a mapper, a reducer, and
    * a mapper and a reducer working together.
    */
    MapDriver<LongWritable, Text, Text, KeyWordCustomMapperWritable> mapDriver;
    ReduceDriver<Text, KeyWordCustomMapperWritable, Text, KeyWordCustomReduceWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, KeyWordCustomMapperWritable, Text, KeyWordCustomReduceWritable> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        TopKeyWordMapper mapper = new TopKeyWordMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, KeyWordCustomMapperWritable>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        TopKeyWordReducer  reducer = new TopKeyWordReducer();
        reduceDriver = new ReduceDriver<Text, KeyWordCustomMapperWritable, Text, KeyWordCustomReduceWritable>();
        reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, KeyWordCustomMapperWritable, Text, KeyWordCustomReduceWritable>();
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
        mapDriver.withOutput(new Text("1testkeyword1"), new KeyWordCustomMapperWritable(1,2));
        mapDriver.withOutput(new Text("\"1testkeyword1\""), new KeyWordCustomMapperWritable(1,2));
        mapDriver.withOutput(new Text("\"gifttax\""), new KeyWordCustomMapperWritable(1,2));
        mapDriver.withOutput(new Text("dog and cat"), new KeyWordCustomMapperWritable(1,2));
        mapDriver.runTest();
    }

    /*
  * Test the reducer.
  */
    @Test
    public void testReducer() throws IOException {

        List<KeyWordCustomMapperWritable> values = new ArrayList<KeyWordCustomMapperWritable>();
        values.add(new KeyWordCustomMapperWritable(1,2));
        values.add(new KeyWordCustomMapperWritable(1,3));
        values.add(new KeyWordCustomMapperWritable(1,4));
        values.add(new KeyWordCustomMapperWritable(1,2));
        reduceDriver.withInput(new Text("1testkeyword1"), values);
        reduceDriver.withOutput(new Text("1testkeyword1"), new KeyWordCustomReduceWritable(4,3));
        reduceDriver.runTest();
    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {
        Path mockPath = new Path("CP_UserSearch_20160306.zip.queries.log");
        mapReduceDriver.setMapInputPath(mockPath);
        mapReduceDriver.withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t1testKeyword1\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"     1testKeyword1    \"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\t\"\"   giftTax   \"\"\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("10_01BCEA5099D956DCE55F349110EEBF72\tdog and cat\t1\t2\tTC\t60\tv1\t366"))
                .withInput(new LongWritable(1), new Text("badData"));
        mapReduceDriver.withOutput(new Text("\"1testkeyword1\""), new KeyWordCustomReduceWritable(1,1));
        mapReduceDriver.withOutput(new Text("\"gifttax\""), new KeyWordCustomReduceWritable(1,1));
        mapReduceDriver.withOutput(new Text("1testkeyword1"), new KeyWordCustomReduceWritable(1,1));
        mapReduceDriver.withOutput(new Text("dog and cat"), new KeyWordCustomReduceWritable(1,1));
        mapReduceDriver.runTest();

    }

}
