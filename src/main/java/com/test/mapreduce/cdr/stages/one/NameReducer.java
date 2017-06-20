package com.test.mapreduce.cdr.stages.one;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class NameReducer extends Reducer<Text, Text, Text, Text> {
    private static final String TAG_DELIMETER = ";";
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
