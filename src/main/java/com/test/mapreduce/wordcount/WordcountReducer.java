package com.test.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 15.06.2017.
 * reducer для подсчета слов
 */
public class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum=0;
        for(IntWritable value: values){
            sum+=value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
