package com.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.beans.IntrospectionException;
import java.io.IOException;

/**
 * Created by Александр Нетяга on 16.06.2017.
 */
public class StageThree {
    private static final String CONTENT_DELIMETER = ";";
    private static final String TAG_DELIMETER = "_";


    public static class joinCdrMapper extends Mapper<Text, Text, Text, Text> {
        private Text valueOut = new Text();

        public static final String TAG = "CDR";

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            valueOut.set(TAG.concat(TAG_DELIMETER.concat(value.toString())));
            context.write(key, valueOut);
        }

    }

    public static class joinNameMapper extends Mapper<Text, Text, Text, Text> {
        private Text valueOut = new Text();

        public static final String TAG = "NAME";

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            valueOut.set(TAG.concat(TAG_DELIMETER.concat(value.toString())));
            context.write(key, valueOut);
        }
    }

    public static class joinReducer extends Reducer<Text, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String name = null;
            String cdrInfo = null;
            for(Text value: values){
                String[] tokens = value.toString().split(TAG_DELIMETER);
                if(tokens[0].equals(joinNameMapper.TAG)){
                    name = tokens[1];
                }
                if(tokens[0].equals(joinCdrMapper.TAG)){
                    cdrInfo = tokens[1];
                }
            }
            if(name != null && cdrInfo != null) {
                keyOut.set(name.concat(CONTENT_DELIMETER.concat(key.toString())));
                valueOut.set(cdrInfo);
                context.write(keyOut,valueOut);
            }

        }
    }
}
