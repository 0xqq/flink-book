package com.github.churtado.flink.com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.SensorReading;
import com.github.churtado.flink.util.SensorSource;
import com.github.churtado.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ConnectStream {
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


        DataStream<SensorReading> first = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0);
                result.location = "paris";

                return result;
            }
        });

        DataStream<SensorReading> second = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0) - 10;
                result.location = "tokyo";

                return result;
            }
        });


        // connect is useful when you want to connect to streams without any condition, such as a key
        // this is non-deterministic
//        ConnectedStreams<SensorReading, SensorReading> connected = first.connect(second);
//        connected.map(new CoMapFunction<SensorReading, SensorReading, Tuple3<String, Double, Long>>() {
//            @Override
//            public Tuple3<String, Double, Long> map1(SensorReading sensorReading) throws Exception {
//                return new Tuple3<>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
//            }
//
//            @Override
//            public Tuple3<String, Double, Long> map2(SensorReading sensorReading) throws Exception {
//                return new Tuple3<>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
//            }
//        }).print();

        // if done this way, all partitions by key will be sent to same operator instance
        // operator instance would have access to keyed state
//        ConnectedStreams<SensorReading, SensorReading> connected = first
//                .connect(second)
//                .keyBy("id", "id");
//        connected.map(new CoMapFunction<SensorReading, SensorReading, Tuple4<String, String, Double, Long>>() {
//            @Override
//            public Tuple4<String, String, Double, Long> map1(SensorReading sensorReading) throws Exception {
//                return new Tuple4<String, String, Double, Long>(sensorReading.id, sensorReading.location, sensorReading.temperature, sensorReading.timestamp);
//            }
//
//            @Override
//            public Tuple4<String, String, Double, Long> map2(SensorReading sensorReading) throws Exception {
//                return new Tuple4<String, String, Double, Long>(sensorReading.id, sensorReading.location, sensorReading.temperature, sensorReading.timestamp);
//            }
//        }).print();

        // In this instance, we broadcast the second, so it would be sent out to all operator instances
        ConnectedStreams<SensorReading, SensorReading> connected = first
                .connect(second.broadcast()) // this is the one difference from above!!!!
                .keyBy("id", "id");
        connected.map(new CoMapFunction<SensorReading, SensorReading, Tuple4<String, String, Double, Long>>() {
            @Override
            public Tuple4<String, String, Double, Long> map1(SensorReading sensorReading) throws Exception {
                return new Tuple4<String, String, Double, Long>(sensorReading.id, sensorReading.location, sensorReading.temperature, sensorReading.timestamp);
            }

            @Override
            public Tuple4<String, String, Double, Long> map2(SensorReading sensorReading) throws Exception {
                return new Tuple4<String, String, Double, Long>(sensorReading.id, sensorReading.location, sensorReading.temperature, sensorReading.timestamp);
            }
        }).print();


        env.execute();

    }
}
