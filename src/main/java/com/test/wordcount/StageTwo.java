package com.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 15.06.2017.
 * нужно для каждого абонента в группировке до дня подсчитать количество звонков и смс утром(6-11), днем(11-19), вечером(19-23), и ночью все остальное
 *
 * то есть на выходе должны быть поля
 * subs_key;yyyy-MM-dd;calls_morning;calls_day;calls_evening;calls_night;sms_morning;sms_day;sms_evening;sms_night
 */
public class StageTwo {

    private static final String CONTENT_DELIMETER = ";";

    public static class CdrMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static final String DELIMETER = "\\|";

        private final int TRANSACTION_IND = 32;
        private final int DATATIME_IND = 2;
        private final int SUBS_KEY_IND = 1;

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(DELIMETER);
            String dateTime = split[DATATIME_IND];
            String date = dateTime.substring(0, 8);
            String time = dateTime.substring(8);
            String txInfo = partOfTheDay(time).concat(split[TRANSACTION_IND]);
            keyOut.set(split[SUBS_KEY_IND].concat(CONTENT_DELIMETER).concat(date));
            valueOut.set(txInfo);
            context.write(keyOut, valueOut);
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

    public static class CdrReducer extends Reducer<Text, Text, Text, Text> {
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] amountVoiceTx = new int[4]; //night-morning-day-evening
            int[] amountSMSTx = new int[4];
            int[] currentTx;
            for (Text tx : values) {
                String txStr = tx.toString();
                currentTx = txStr.charAt(1) == 'V' ? amountVoiceTx : amountSMSTx;
                currentTx[getIndex(txStr.charAt(0))]++;
            }
            StringBuffer buffer = new StringBuffer();
            String[] keysToken = key.toString().split(CONTENT_DELIMETER);
            keyOut.set(keysToken[0]);
            valueOut.set(keysToken[1].concat(appendBuffer(buffer, amountVoiceTx, amountSMSTx)));
            context.write(keyOut, valueOut);

        }

        private int getIndex(char p) {
            switch (p) {
                case 'n':
                    return 0;
                case 'm':
                    return 1;
                case 'd':
                    return 2;
                case 'e':
                    return 3;
            }
            return 0;
        }

        private String appendBuffer(StringBuffer buffer, int[] voiceTx, int[] smsTx) {
            for (int amount : voiceTx)
                buffer.append(CONTENT_DELIMETER)
                        .append(amount);

            for (int amount : smsTx)
                buffer.append(CONTENT_DELIMETER)
                        .append(amount);
            return buffer.toString();
        }
    }

}
