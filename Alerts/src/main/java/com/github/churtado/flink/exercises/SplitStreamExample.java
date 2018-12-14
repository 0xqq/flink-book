package com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamExample {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(4); // creates 4 copies of everything, but you can set this for specific operators
        // setting the parallelism of an operator overrides the above setting

        // configure the watermark interval at every second in this case
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                // assign timestamps and watermarks required for event time
                // .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)))
                //.assignTimestampsAndWatermarks(new PeriodicAssigner()); // if you want to create a periodic assigner
                .assignTimestampsAndWatermarks(new PunctuatedAssigner());
                //assingAscendingWatermarks not available???

        readings.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return false;
            }
        }).setParallelism(2); // this will override the global parallelism of 4

        SplitStream<SensorReading> split =  readings
                .shuffle()  // random
                .rebalance() // balance it out to successor tasks (round robin)
                .rescale()   // almost round robin, only distributed to some, not all tasks
                .broadcast() // send everything all copies of the downstream operator
                .global()    // send everything to first copy of downstream operator
                // also you can define your own, I won't bother for now
                .split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {

                // you can put lots of conditions here
                List<String> output = new ArrayList<String>();
                if(sensorReading.temperature < 100) {
                    output.add("low");
                } else {
                    output.add("high");
                }
                return output;
            }
        });

        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> high = split.select("high");

        high.map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
            }
        }).print().setParallelism(1);

//        low.map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
//            @Override
//            public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
//                return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
//            }
//        }).print();

        env.execute();

    }
}
