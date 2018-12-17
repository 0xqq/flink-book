package com.github.churtado.flink.configuration;

import com.github.churtado.flink.configuration.util.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SmokeAlerts {

    // you have 2 streams, 1 for smoke level and another for temperature
    // connect the streams and emit alerts based on some threshold
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
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time

        DataStream<SmokeLevel> smokeLevel = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1); // warning: no checkpointing

        // this example uses co flatmap to join 2 streams, one with temp, and the other
        // with a smoke level stream. When the smoke level and temp is too high, then it emits an alarm
        // key sensors by id
        readings
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                })
                // connecting temp readings with smoke level
                .connect(smokeLevel.broadcast())
                // raising alerts on temp and smoke level via co flatmap
                .flatMap(new RaiseAlertFlatmap())
                // mapping to tuple and outputting
                .map(new MapFunction<Alert, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Alert alert) throws Exception {
                        return new Tuple2<>(alert.message, alert.timestamp);
                    }
                }).print();

        env.execute();
    }

    static class RaiseAlertFlatmap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

        SmokeLevel level = SmokeLevel.Low;

        @Override
        public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
            if(level.equals(SmokeLevel.Low) && sensorReading.temperature > 100) {

                Alert alert = new Alert();
                alert.timestamp = sensorReading.timestamp;
                alert.message = "Risk of fire!";
                collector.collect(alert);
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
            level = smokeLevel;
        }
    }
}
