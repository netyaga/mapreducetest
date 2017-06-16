package com.test.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Александр Нетяга on 15.06.2017.
 */
public class TestCdr {


    private StageTwo.CdrMapper mapper = new StageTwo.CdrMapper();
    private MapDriver<LongWritable, Text, Text, Text> mapperDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MultipleInputsMapReduceDriver<Text, Text, Text, Text> mapReduceDriver;
    public static final String FILE_PATH = "input\\cdr.csv";

    @Before
    public void setUp() {

        StageTwo.CdrReducer reducer = new StageTwo.CdrReducer();
        mapperDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver(reducer);
        mapReduceDriver.addMapper(mapper);

    }

    @Test
    public void testMapReduceMany() throws IOException {
        mapReduceDriver.withInput(mapper, new LongWritable(), new Text("0\t01|9621266117|20170101235959|79621009999||NGMO|2||100|74955042222|79605550000|||041611|I||||||250995148305377|356258066657110||34967|||22101||A|||11|V|81079006658016|274||||SE|||G|16|||||||||||||||Y|CF0279.ysakhalinsk.20160831202215||1|||||79006658016||||+@@+1{79098509981}2{1}3{1}4{0}5{79621009999}6{41310BD7B2}7{79006658016}"));
        mapReduceDriver.withInput(mapper, new LongWritable(), new Text("0\t01|9621266117|20170101235959|79621009999||NGMO|2||100|74955042222|79605550000|||041611|I||||||250995148305377|356258066657110||34967|||22101||A|||11|V|81079006658016|274||||SE|||G|16|||||||||||||||Y|CF0279.ysakhalinsk.20160831202215||1|||||79006658016||||+@@+1{79098509981}2{1}3{1}4{0}5{79621009999}6{41310BD7B2}7{79006658016}"));
        mapReduceDriver.withInput(mapper, new LongWritable(), new Text("0\t01|9621266117|20170101235959|79621009999||NGMO|2||100|74955042222|79605550000|||041611|I||||||250995148305377|356258066657110||34967|||22101||A|||11|S|81079006658016|274||||SE|||G|16|||||||||||||||Y|CF0279.ysakhalinsk.20160831202215||1|||||79006658016||||+@@+1{79098509981}2{1}3{1}4{0}5{79621009999}6{41310BD7B2}7{79006658016}"));
        mapReduceDriver.withOutput(new Text("9621266117"), new Text("20170101;2;0;0;0;1;0;0;0"));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceSingle() throws IOException {
        mapReduceDriver.withInput(mapper, new LongWritable(), new Text("0\t01|9621266117|20170101235959|79621009999||NGMO|2||100|74955042222|79605550000|||041611|I||||||250995148305377|356258066657110||34967|||22101||A|||11|V|81079006658016|274||||SE|||G|16|||||||||||||||Y|CF0279.ysakhalinsk.20160831202215||1|||||79006658016||||+@@+1{79098509981}2{1}3{1}4{0}5{79621009999}6{41310BD7B2}7{79006658016}"));
        mapReduceDriver.withOutput(new Text("9621266117"), new Text("20170101;1;0;0;0;0;0;0;0"));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceFile(){
        try{
            Scanner sc = new Scanner(new File(FILE_PATH));
            while(sc.hasNext()){
                mapReduceDriver.withInput(mapper ,new LongWritable(), new Text(sc.nextLine()));
            }

            final List<Pair<Text,Text>> result = mapReduceDriver.run();
            for(Pair <Text,Text> element: result){
                System.out.println(element.getFirst().toString()+element.getSecond().toString());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }


    }
}
