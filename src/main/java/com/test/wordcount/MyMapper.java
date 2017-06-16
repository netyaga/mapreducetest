package com.test.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 15.06.2017.
 */
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text wordOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] words = value.toString().split(" ");
        for(String word: words){
            wordOut.set(word);
            context.write(wordOut, one);
        }
    }
}
