package com.test.mapreduce.cdr.stages.two;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Александр Нетяга on 20.06.2017.
 */
public class CdrReducer extends Reducer<Text, Text, Text, Text> {
    private static final String CONTENT_DELIMETER = ";";
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
