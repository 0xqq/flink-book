package com.github.churtado.flink.window;

import com.github.churtado.flink.window.util.SensorReading;
import com.github.churtado.flink.window.util.SensorSource;
import com.github.churtado.flink.window.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class WindowingExample {

    public static class AverageAccumulator {
        double count;
        double sum;
    }

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

        KeyedStream<SensorReading, String> keyed = readings
                .keyBy(new KeySelector<SensorReading, String>() {

                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                });

        keyed
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                // .timeWindow(Time.seconds(1))  // shortcut for window.(TumblingEventTimeWindows.of(size))

                // 00:15:00, 01:15:00, 02:15:00 because of 15 minute offset
                //.timeWindow(Time.hours(1), Time.minutes(15)) // group readings in 1 hour windows with 15 min offset

                .aggregate(new AggregateFunction <SensorReading, AverageAccumulator, Double>() {

                    @Override
                    public AverageAccumulator createAccumulator() {
                        return new AverageAccumulator();
                    }

                    @Override
                    public AverageAccumulator add(SensorReading sensorReading, AverageAccumulator averageAccumulator) {
                        averageAccumulator.sum += sensorReading.temperature;
                        averageAccumulator.count ++;
                        return averageAccumulator;
                    }

                    @Override
                    public Double getResult(AverageAccumulator averageAccumulator) {

                        return averageAccumulator.sum / averageAccumulator.count;
                    }

                    @Override
                    public AverageAccumulator merge(AverageAccumulator averageAccumulator, AverageAccumulator acc1) {
                        averageAccumulator.sum += acc1.sum;
                        averageAccumulator.count += acc1.count;

                        return averageAccumulator;
                    }
                }).print();

        env.execute();
    }

}
