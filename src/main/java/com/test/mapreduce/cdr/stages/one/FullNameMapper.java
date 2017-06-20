package com.test.mapreduce.cdr.stages.one;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class FullNameMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String TAG_DELIMETER = ";";
    private final String DELIMETER = "\u0001";
    public static final String TAG = "NAME";

    private final int CUST_FILLNAME = 14;
    private final int BAN_KEY = 0;

    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(DELIMETER);
        keyOut.set(split[BAN_KEY]);
        valueOut.set(TAG.concat(TAG_DELIMETER).concat(split[CUST_FILLNAME]));
        context.write(keyOut, valueOut);
    }
}
