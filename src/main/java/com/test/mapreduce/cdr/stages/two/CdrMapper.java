package com.test.mapreduce.cdr.stages.two;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class CdrMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String CONTENT_DELIMETER = ";";
    public static final String DELIMETER = "\\|";

    private final int TRANSACTION_IND = 32;
    private final int DATATIME_IND = 2;
    private final int SUBS_KEY_IND = 1;

    Set<String> filterDate= new HashSet<String>();

    private Text keyOut = new Text();
    private Text valueOut = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(DELIMETER);
        String dateTime = split[DATATIME_IND];
        String date = dateTime.substring(0, 8);
        String time = dateTime.substring(8);
        if(filterDate.contains(date)) {
            String txInfo = partOfTheDay(time).concat(split[TRANSACTION_IND]);
            keyOut.set(split[SUBS_KEY_IND].concat(CONTENT_DELIMETER).concat(date));
            valueOut.set(txInfo);
            context.write(keyOut, valueOut);
        }
    }

    @Override
    protected void setup(Context context) throws  IOException, InterruptedException{
        try{
            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if( cacheFiles!=null && cacheFiles.length >0){
                BufferedReader reader = new BufferedReader( new FileReader(cacheFiles[0].toString()));
                String line;
                while((line = reader.readLine())!= null){
                    filterDate.add(line);
                }
            }
        }
        catch (IOException ex){
            System.err.println("Exception in mapper setup() " + ex.getMessage());
        }
    }

    private String partOfTheDay(String time) {
        int hour = Integer.parseInt(time.substring(0, 1));
        if (hour >= 6 && hour < 11)
            return "m";
        else if (hour >= 11 && hour < 19)
            return "d";
        else if (hour >= 19 && hour < 23)
            return "e";
        else
            return "n";
    }

}
