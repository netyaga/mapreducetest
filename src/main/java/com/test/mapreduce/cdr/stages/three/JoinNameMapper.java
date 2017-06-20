package com.test.mapreduce.cdr.stages.three;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class JoinNameMapper extends Mapper<Text, Text, Text, Text> {
    private static final String TAG_DELIMETER = "_";
    private Text valueOut = new Text();

    public static final String TAG = "NAME";

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        valueOut.set(TAG.concat(TAG_DELIMETER.concat(value.toString())));
        context.write(key, valueOut);
    }
}
