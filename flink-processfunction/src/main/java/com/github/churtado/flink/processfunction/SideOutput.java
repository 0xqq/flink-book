package com.github.churtado.flink.processfunction;

import com.github.churtado.flink.processfunction.util.SensorReading;
import com.github.churtado.flink.processfunction.util.SensorSource;
import com.github.churtado.flink.processfunction.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * This code takes the sensor readings and detects freezing temperatures. Once the temp is detected,
 * it's sent to a side output that we can reference through an output tag. We print both streams
 */

public class SideOutput {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Check the watermark every 5 seconds
        env.getConfig().setAutoWatermarkInterval(5000);

        // configure the watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // defining an output tag for a side output for freezing temperatures
        final OutputTag<String> freezingAlarmOutput = new OutputTag<String>("freezing-alarms"){};

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time

        // using a process function to emit warnings if temp monotonically increases within a second
        DataStream<String> freezingAlarmsStream = readings.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.id;
            }
        }).process(new KeyedProcessFunction<String, SensorReading, String>() {

            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
                if (sensorReading.temperature < 32.0) {
                    // output the alarm value to side output
                    context.output(freezingAlarmOutput, "detected a freezing temperature of " + sensorReading.temperature + " from sensor " + sensorReading.id);
                }
            }
        }).getSideOutput(freezingAlarmOutput);

        // retrieve and print the freezing alarms
        freezingAlarmsStream.print();

        // print the main output
        readings.map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
            }
        }).print();

        env.execute();
    }


}
