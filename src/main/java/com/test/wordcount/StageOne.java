package com.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 15.06.2017.
 * делаем словарь соответствия subs_key и fullname
 */
public class StageOne {
    private static final String TAG_DELIMETER = ";";

    public static class FullNameMapper extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class SubsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final String DELIMETER = "\u0001";
        public static final String TAG = "SUBS";

        private final int SUBS_KEY = 1;
        private final int BAN_KEY = 0;

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(DELIMETER);
            keyOut.set(split[BAN_KEY]);
            valueOut.set(TAG.concat(TAG_DELIMETER).concat(split[SUBS_KEY]));
            context.write(keyOut, valueOut);
        }
    }

    public static class NameReducer extends Reducer<Text, Text, Text, Text> {
        private Text valueOut = new Text();
        private Text keyOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fullName = null;
            String subsKey = null;
            for (Text value : values) {
                String[] split = value.toString().split(TAG_DELIMETER);
                if (split[0].equals(FullNameMapper.TAG))
                    fullName = split[1];
                if (split[0].equals(SubsMapper.TAG))
                    subsKey = split[1];
            }
            if (fullName != null && subsKey != null)
                keyOut.set(subsKey);
            valueOut.set(fullName);
            context.write(keyOut, valueOut);
        }
    }

}
