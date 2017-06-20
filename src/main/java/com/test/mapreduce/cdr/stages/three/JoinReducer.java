package com.test.mapreduce.cdr.stages.three;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private static final String CONTENT_DELIMETER = ";";
    private static final String TAG_DELIMETER = "_";
    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name = null;
        String cdrInfo = null;
        for (Text value : values) {
            String[] tokens = value.toString().split(TAG_DELIMETER);
            if (tokens[0].equals(JoinNameMapper.TAG)) {
                name = tokens[1];
            }
            if (tokens[0].equals(JoinCdrMapper.TAG)) {
                cdrInfo = tokens[1];
            }
        }
        if (name != null && cdrInfo != null) {
            keyOut.set(name.concat(CONTENT_DELIMETER.concat(key.toString())));
            valueOut.set(cdrInfo);
            context.write(keyOut, valueOut);
        }

    }
}
