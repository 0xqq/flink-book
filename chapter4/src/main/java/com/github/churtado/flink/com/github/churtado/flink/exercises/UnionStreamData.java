package com.github.churtado.flink.com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.SensorReading;
import com.github.churtado.flink.util.SensorSource;
import com.github.churtado.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UnionStreamData {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // configure the watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                .setParallelism(4) // ingest through a source
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time


        DataStream<SensorReading> parisStream = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = "paris_"+reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0);

                return result;
            }
        });

        DataStream<SensorReading> tokyoStream = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = "tokyo_"+reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0) - 10;

                return result;
            }
        });

        DataStream<SensorReading> rioStream = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = "rio_"+reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0) + 10;

                return result;
            }
        });

        DataStream<SensorReading> allCities = parisStream.union(tokyoStream, rioStream);

        allCities.map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
            }
        }).print();

        env.execute();

    }
}
