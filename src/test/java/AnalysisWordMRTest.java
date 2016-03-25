import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by George on 3/24/2016.
 */
public class AnalysisWordMRTest {  /*
    * Declare harnesses that let you test a mapper, a reducer, and
    * a mapper and a reducer working together.
    */
    MapDriver<Text, Text, DoubleWritable, Text> mapDriver;
    ReduceDriver<DoubleWritable, Text, DoubleWritable, Text> reduceDriver;
    MapReduceDriver<Text, Text, DoubleWritable, Text, DoubleWritable, Text> mapReduceDriver;


    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        AnalysisKeyWordMapper mapper = new AnalysisKeyWordMapper();
        mapDriver = new MapDriver<Text, Text, DoubleWritable, Text>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        AnalysisKeyWordReducer reducer = new AnalysisKeyWordReducer();
        reduceDriver = new ReduceDriver<DoubleWritable, Text, DoubleWritable, Text>();
        reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<Text, Text, DoubleWritable, Text, DoubleWritable, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper1() throws IOException, URISyntaxException {
        mapDriver.addCacheFile(this.getClass().getResource("testFile.txt").toURI());
        mapDriver.withInput(new Text("testKeyword0"), new Text("4\t0"))
                .withInput(new Text("testKeyword1"), new Text("3\t2"))
                .withInput(new Text("testKeyword2"), new Text("5\t3"))
                .withInput(new Text("testKeyword3"), new Text("6\t4"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapDriver.withOutput(new DoubleWritable(new Double(0/((4 * 11)/15))),new Text("testKeyword0"));
        mapDriver.withOutput(new DoubleWritable(new Double(2/((3 * 12)/15))),new Text("testKeyword1"));
        mapDriver.withOutput(new DoubleWritable(new Double(3/((5 * 10)/15))),new Text("testKeyword2"));
        mapDriver.withOutput(new DoubleWritable(new Double(4/((6 * 9)/15))),new Text("testKeyword3"));
        mapDriver.runTest();
    }


    /*
  * Test the reducer.
  */
    @Test
    public void testReducer() throws IOException {

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("testKeyword"));
        reduceDriver.withInput(new DoubleWritable(10d), values);
        reduceDriver.withOutput( new DoubleWritable(10d),new Text("testKeyword"));
        reduceDriver.runTest();
    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce1() throws IOException, URISyntaxException {
        mapReduceDriver.addCacheFile(this.getClass().getResource("testFile.txt").toURI());
        mapReduceDriver.withInput(new Text("testKeyword0"), new Text("4\t0"))
                .withInput(new Text("testKeyword1"), new Text("3\t2"))
                .withInput(new Text("testKeyword2"), new Text("5\t3"))
                .withInput(new Text("testKeyword3"), new Text("6\t4"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapReduceDriver.withOutput(new DoubleWritable(new Double(0/((4 * 11)/15))),new Text("testKeyword0"));
        mapReduceDriver.withOutput(new DoubleWritable(new Double(2/((3 * 12)/15))),new Text("testKeyword1"));
        mapReduceDriver.withOutput(new DoubleWritable(new Double(3/((5 * 10)/15))),new Text("testKeyword2"));
        mapReduceDriver.withOutput(new DoubleWritable(new Double(4/((6 * 9)/15))),new Text("testKeyword3"));
        mapReduceDriver.runTest();
    }


}

