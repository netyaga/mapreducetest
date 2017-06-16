package com.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;


/**
 * Created by Александр Нетяга on 15.06.2017.
 */
public class TestName {
    private StageOne.FullNameMapper fullNameMapper = new StageOne.FullNameMapper();
    private StageOne.SubsMapper subsMapper = new StageOne.SubsMapper();
    private MapDriver<LongWritable, Text, Text, Text> nameMapDriver;
    private MapDriver<LongWritable, Text, Text, Text> subsMapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mapReduceDriver;
    private static final String DIM_BAN = "input\\DIM_BAN.csv";
    private static final String DIM_SUBS = "input\\DIM_SUBSCRIBER.csv";



    @Before
    public void setUp() throws Exception {
        StageOne.NameReducer reducer = new StageOne.NameReducer();
        nameMapDriver = MapDriver.newMapDriver(fullNameMapper);
        subsMapDriver = MapDriver.newMapDriver(subsMapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);
        mapReduceDriver.addMapper(fullNameMapper);
        mapReduceDriver.addMapper(subsMapper);
    }

    @Test
    public void mapReduceFile() {
        try {
            Scanner sc = new Scanner(new File(DIM_BAN));
            while(sc.hasNext()){
                mapReduceDriver.withInput(fullNameMapper, new LongWritable(), new Text(sc.nextLine()));
            }
            sc = new Scanner(new File(DIM_SUBS));
            while(sc.hasNext()){
                mapReduceDriver.withInput(subsMapper, new LongWritable(), new Text(sc.nextLine()));
            }
            mapReduceDriver.withOutput(new Text("9050000021"), new Text("Г-жа Зан Людмила Георгиевна"));
            mapReduceDriver.withOutput(new Text("9601411003"), new Text("Г-н Сенников Геннадий Николаевич"));
            mapReduceDriver.withOutput(new Text("9617452271"), new Text("Г-н Постой Сергей Петрович"));
            mapReduceDriver.runTest();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Test
    public void mapReduce() throws IOException{
        mapReduceDriver.withInput(fullNameMapper, new LongWritable(), new Text("100102202\u0001F531002\u000113.0\u0001ABK\u0001CAN\u0001GCAN\u0001N\u0001N\u00012005-10-28 00:00:00.0\u00011\u00011\u0001NULL\u00019503 501692\u00011\u0001Г-жа Зан Людмила Георгиевна\u00011977-10-31 00:00:00.0\u0001с. Боград, ул. Школьная, д. 12, Кв. 7\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001645.0\u00017.0\u00010.0\u0001N\u00012005-10-28 00:00:00.0\u00012007-08-03 00:00:00.0\u0001NULL\u0001NULL\u0001NULL\u00012005-11-12 00:00:00.0\u0001NULL\u00010.0\u00010.0\u00011.0\u00011.0\u00010.0\u00010.0\u0001NULL\u00012012-06-02 23:41:51.0\u0001NULL\u0001Боград\u0001NULL\u0001NULL\u0001F\u0001-99.0\u0001EXS\u0001NULL\u0001NULL\u0001D\u00012004-03-05 00:00:00.0\u0001Боградским РОВД\u0001NULL\u0001NULL\u0001NULL\u0001PABK\n"));
        mapReduceDriver.withInput(fullNameMapper, new LongWritable(), new Text("100102250\u0001F531002\u000113.0\u0001ABK\u0001CAN\u0001GCAN\u0001N\u0001N\u00012005-10-28 00:00:00.0\u00011\u00011\u0001NULL\u00019502 290862\u00011\u0001Г-н Сенников Геннадий Николаевич\u00011951-12-24 00:00:00.0\u0001пос. Аскиз, ул. Советская, д. 3а, Кв. 1\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001366.0\u00017.0\u00010.0\u0001NULL\u00012005-10-28 00:00:00.0\u00012006-10-28 00:00:00.0\u0001NULL\u0001NULL\u0001NULL\u00012005-11-12 00:00:00.0\u0001NULL\u0001NULL\u00010.0\u00011.0\u00011.0\u00010.0\u00010.0\u0001NULL\u00012012-06-02 23:41:51.0\u0001NULL\u0001Аскиз\u0001NULL\u0001NULL\u0001M\u0001-99.0\u0001EXS\u0001NULL\u0001NULL\u0001D\u00012002-09-18 00:00:00.0\u0001Аскизским ОВД\u0001NULL\u0001NULL\u0001NULL\u0001PABK\n"));
        mapReduceDriver.withInput(fullNameMapper, new LongWritable(), new Text("100102289\u0001F531002\u000113.0\u0001ABK\u0001CAN\u0001GCAN\u0001N\u0001N\u00012005-10-28 00:00:00.0\u00011\u00011\u0001NULL\u00019502 353219\u00011\u0001Г-н Постой Сергей Петрович\u00011969-01-14 00:00:00.0\u0001г. Абакан, ул. Ленина, д. 140, Кв. а\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001NULL\u0001379.0\u00017.0\u00010.0\u0001N\u00012005-10-28 00:00:00.0\u00012006-11-10 00:00:00.0\u0001NULL\u0001NULL\u0001NULL\u00012005-11-21 00:00:00.0\u0001NULL\u00010.0\u00010.0\u00011.0\u00011.0\u00010.0\u00010.0\u0001NULL\u00012012-06-02 23:41:51.0\u0001NULL\u0001Абакан\u0001NULL\u0001NULL\u0001M\u0001-99.0\u0001EXS\u0001NULL\u0001NULL\u0001D\u00012001-01-13 00:00:00.0\u0001УВД г.Абакана\u0001NULL\u0001NULL\u0001NULL\u0001PABK\n"));

        mapReduceDriver.withInput(subsMapper, new LongWritable(), new Text("100102202\u00019050000021\u0001F500000\u0001ABK\u000113.0\u000119FABT01\u000119FABT01\u0001A\u00012009-08-25 00:00:00.0\u000119FABT01\u00012005-10-28 00:00:00.0\u0001CAN\u0001GCAN\u00012005-12-16 00:00:00.0\u00012005-12-16 00:00:00.0\u00011398.0\u00010\u00012005-10-28 00:00:00.0\u00011.0\u0001G\u00011.0\u00010.0\u00010.0\u0001\\N\u0001\\N\u0001NULL\u0001NULL\u00012012-06-02 02:20:46.0\u00012005-10-29 04:02:30.0\u0001INV\u00012005-10-28 00:00:00.0\u0001A\u0001-77\u00012009-09-12 00:00:00.0\u00012009-08-25 00:00:00.0\u0001EXS\u0001F500000\u00012005-10-28 00:00:00.0\u00011.00006035E8\u00019039170016\u00015.6360233E7\u0001-99.0\u0001-99.0\u00010.0\u0001P\u0001PABK\n"));
        mapReduceDriver.withInput(subsMapper, new LongWritable(), new Text("100102250\u00019601411003\u0001F531002\u0001ABK\u000113.0\u000119BOOM61\u000119FABT01\u0001A\u00012008-12-12 00:00:00.0\u000119FABT01\u00012005-11-09 00:00:00.0\u0001CAN\u0001GCAN\u00012005-11-09 00:00:00.0\u00012005-11-18 00:00:00.0\u00011130.0\u00010\u00012005-12-04 00:00:00.0\u00011.0\u0001G\u00011.0\u00010.0\u00010.0\u0001\\N\u0001\\N\u0001NULL\u0001NULL\u00012012-06-02 02:20:46.0\u00012005-10-29 04:04:07.0\u0001INV\u00012005-11-09 00:00:00.0\u0001A\u0001-77\u00012008-12-14 00:00:00.0\u00012008-12-12 00:00:00.0\u0001EXS\u0001F531002\u00012005-10-28 00:00:00.0\u00011.00102273E8\u00019617394881\u00017.365108E7\u00011.4054396E7\u00011.4054396E7\u00010.0\u0001P\u0001PABK\n"));
        mapReduceDriver.withInput(subsMapper, new LongWritable(), new Text("100102289\u00019617452271\u0001-99\u0001ABK\u000113.0\u000119FABT01\u000119FABT01\u0001A\u00012007-11-24 00:00:00.0\u000119FABT01\u00012005-11-19 00:00:00.0\u0001CAN\u0001GCAN\u0001NULL\u0001NULL\u0001736.0\u00010\u00012005-10-28 00:00:00.0\u00011.0\u0001G\u00011.0\u00010.0\u00010.0\u0001\\N\u0001\\N\u0001NULL\u0001NULL\u00012012-06-02 02:20:46.0\u00012005-10-29 04:04:07.0\u0001INV\u00012005-11-19 00:00:00.0\u0001A\u0001-77\u00012007-11-28 00:00:00.0\u00012007-11-24 00:00:00.0\u0001EXS\u0001-99\u00012005-10-28 00:00:00.0\u00011.00102277E8\u00019617394908\u00015.6314543E7\u0001-99.0\u0001-99.0\u00010.0\u0001P\u0001PABK\n"));

        mapReduceDriver.withOutput(new Text("9050000021"), new Text("Г-жа Зан Людмила Георгиевна"));
        mapReduceDriver.withOutput(new Text("9601411003"), new Text("Г-н Сенников Геннадий Николаевич"));
        mapReduceDriver.withOutput(new Text("9617452271"), new Text("Г-н Постой Сергей Петрович"));

        mapReduceDriver.runTest();
    }
}
