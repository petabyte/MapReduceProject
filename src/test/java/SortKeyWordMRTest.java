import org.apache.hadoop.io.IntWritable;
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
public class SortKeyWordMRTest
{  /*
    * Declare harnesses that let you test a mapper, a reducer, and
    * a mapper and a reducer working together.
    */
    MapDriver<Text, Text, IntWritable, Text> mapSort1Driver;
    MapDriver<Text, Text, IntWritable, Text> mapSort2Driver;
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
    MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver1;
    MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver2;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        SortKeyTopWordMapper mapper1 = new SortKeyTopWordMapper();
        mapSort1Driver = new MapDriver<Text, Text, IntWritable, Text>();
        mapSort1Driver.setMapper(mapper1);

    /*
     * Set up the mapper test harness.
     */
        SortTopWordTaxSeasonMapper mapper2 = new SortTopWordTaxSeasonMapper();
        mapSort2Driver = new MapDriver<Text, Text, IntWritable, Text>();
        mapSort2Driver.setMapper(mapper2);
    /*
     * Set up the reducer test harness.
     */
        KeyWordReducer reducer = new KeyWordReducer();
        reduceDriver = new ReduceDriver<IntWritable, Text, IntWritable, Text>();
        reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver1 = new MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text>();
        mapReduceDriver1.setMapper(mapper1);
        mapReduceDriver1.setReducer(reducer);
    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver2 = new MapReduceDriver<Text, Text, IntWritable, Text, IntWritable, Text>();
        mapReduceDriver2.setMapper(mapper2);
        mapReduceDriver2.setReducer(reducer);

    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper1() throws IOException {
        mapSort1Driver.withInput(new Text("testKeyword0"), new Text("10\t15"))
                .withInput(new Text("testKeyword1"), new Text("11\t14"))
                .withInput(new Text("testKeyword2"), new Text("12\t13"))
                .withInput(new Text("testKeyword3"), new Text("13\t12"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapSort1Driver.withOutput(new IntWritable(10),new Text("testKeyword0"));
        mapSort1Driver.withOutput(new IntWritable(11),new Text("testKeyword1"));
        mapSort1Driver.withOutput(new IntWritable(12),new Text("testKeyword2"));
        mapSort1Driver.withOutput(new IntWritable(13),new Text("testKeyword3"));
        mapSort1Driver.runTest();
    }

    @Test
    public void testMapper2() throws IOException {
        mapSort2Driver.withInput(new Text("testKeyword0"), new Text("10\t15"))
                .withInput(new Text("testKeyword1"), new Text("11\t14"))
                .withInput(new Text("testKeyword2"), new Text("12\t13"))
                .withInput(new Text("testKeyword3"), new Text("13\t12"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapSort2Driver.withOutput(new IntWritable(15),new Text("testKeyword0"));
        mapSort2Driver.withOutput(new IntWritable(14),new Text("testKeyword1"));
        mapSort2Driver.withOutput(new IntWritable(13),new Text("testKeyword2"));
        mapSort2Driver.withOutput(new IntWritable(12),new Text("testKeyword3"));
        mapSort2Driver.runTest();
    }

    /*
  * Test the reducer.
  */
    @Test
    public void testReducer() throws IOException {

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("testKeyword"));
        reduceDriver.withInput(new IntWritable(10), values);
        reduceDriver.withOutput( new IntWritable(10),new Text("testKeyword"));
        reduceDriver.runTest();
    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce1() throws IOException {

        mapReduceDriver1.withInput(new Text("testKeyword0"), new Text("10\t15"))
                .withInput(new Text("testKeyword1"), new Text("11\t14"))
                .withInput(new Text("testKeyword2"), new Text("12\t13"))
                .withInput(new Text("testKeyword3"), new Text("13\t12"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapReduceDriver1.withOutput(new IntWritable(10),new Text("testKeyword0"));
        mapReduceDriver1.withOutput(new IntWritable(11),new Text("testKeyword1"));
        mapReduceDriver1.withOutput(new IntWritable(12),new Text("testKeyword2"));
        mapReduceDriver1.withOutput(new IntWritable(13),new Text("testKeyword3"));
        mapReduceDriver1.runTest();

    }

    @Test
    public void testMapReduce2() throws IOException {

        mapReduceDriver2.withInput(new Text("testKeyword0"), new Text("10\t15"))
                .withInput(new Text("testKeyword1"), new Text("11\t14"))
                .withInput(new Text("testKeyword2"), new Text("12\t13"))
                .withInput(new Text("testKeyword3"), new Text("13\t12"))
                .withInput(new Text("testKeyWord4"), new Text("badData"));
        mapReduceDriver2.withOutput(new IntWritable(12),new Text("testKeyword3"));
        mapReduceDriver2.withOutput(new IntWritable(13),new Text("testKeyword2"));
        mapReduceDriver2.withOutput(new IntWritable(14),new Text("testKeyword1"));
        mapReduceDriver2.withOutput(new IntWritable(15),new Text("testKeyword0"));
        mapReduceDriver2.runTest();

    }

}

