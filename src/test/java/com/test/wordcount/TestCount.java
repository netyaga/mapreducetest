package com.test.wordcount;

import org.apache.hadoop.conf.Configuration;
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
 * Created by Александр Нетяга on 15.06.2017.
 * тестирование мр-подсчет слов в тесте
 */
public class TestCount {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable,Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws IOException{
        MyMapper mapper = new MyMapper();
        MyReducer reducer = new MyReducer();
        mapDriver = mapDriver.newMapDriver(mapper);
        reduceDriver = reduceDriver.newReduceDriver(reducer);
        mapReduceDriver = mapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void testMapper() throws IOException{
        mapDriver.withInput(new LongWritable(), new Text("1 11 111 1"));
        mapDriver.withOutput(new Text("1"), new IntWritable(1));
        mapDriver.withOutput(new Text("1"), new IntWritable(1));
        mapDriver.withOutput(new Text("11"), new IntWritable(1));
        mapDriver.withOutput(new Text("111"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException{
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(3));
        values.add(new IntWritable(2));
        reduceDriver.withInput(new Text("text"), values);
        reduceDriver.withOutput(new Text("text"), new IntWritable(6));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new LongWritable(), new Text("1 1 11 111 11 1 1"));
        mapReduceDriver.withOutput(new Text("1"), new IntWritable(4));
        mapReduceDriver.withOutput(new Text("11"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("111"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
