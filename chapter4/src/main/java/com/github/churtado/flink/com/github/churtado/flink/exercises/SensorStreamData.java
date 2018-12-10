package com.github.churtado.flink.com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SensorStreamData {

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

        // convert to celsius
        DataStream<SensorReading> celsius = readings.map(new MapFunction<SensorReading, SensorReading>() {
            @Override
            public SensorReading map(SensorReading reading) throws Exception {
                SensorReading result = new SensorReading();
                result.id = reading.id;
                result.timestamp = reading.timestamp;
                result.temperature = (reading.temperature - 32) * (5.0 / 9.0);

                return result;
            }
        });

        // this is to test flatmap and filter
//        celsius
//                .flatMap(new FlatMapFunction<SensorReading, String>() {
//                    @Override
//                    public void flatMap(SensorReading sensorReading, Collector<String> collector) throws Exception {
//                        String[] results = sensorReading.id.split("_");
//                        for(String result: results) {
//                            if(result != "sensor") {
//                                collector.collect(result);
//                            }
//                        }
//                    }
//                }).returns(String.class).print();

        // this is to output the 5-second window averages
//        celsius
//                .keyBy(new KeySelector<SensorReading, String>() {
//                    @Override
//                    public String getKey(SensorReading reading) throws Exception {
//                        return reading.id;
//                    }
//                }).timeWindow(Time.seconds(5))
//                .apply(new AverageFunction())
//                .map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
//                    @Override
//                    public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
//                        return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
//                    }
//                })
//                .print();

        // showing rolling aggregators by key, remember, you need a KeyedStream
//        celsius
//                .keyBy(new KeySelector<SensorReading, String>() {
//                    @Override
//                    public String getKey(SensorReading reading) throws Exception {
//                        return reading.id;
//                    }
//                }).map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
//                    @Override
//                    public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
//                        return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
//                    }
//                }).keyBy(0).sum(2) // maintains a rolling sum for each of the 10 keys
//                .print();

        // showing the reduce function, which is a generalization of rolling aggregators like the above. Calculating max
        celsius
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading) throws Exception {
                        return sensorReading.id.equals("sensor_9");
                    }
                })
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading reading) throws Exception {
                        return reading.id;
                    }
                })
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading r1, SensorReading r2) throws Exception {

                        Double highestTemp = Math.max(r1.temperature, r2.temperature);
                        SensorReading result = new SensorReading();
                        result.id = r1.id;
                        result.temperature = highestTemp;

                        if(highestTemp == r1.temperature) {
                            result.timestamp = r1.timestamp;
                        } else {
                            result.timestamp = r2.timestamp;
                        }

                        return result;

                    }
                })
                .map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                        return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
                    }
                }).print();

        env.execute();

    }

    static class AverageFunction implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<SensorReading> collector) throws Exception {

            Double sum = 0.0;
            Double count = 0.0;

            for(SensorReading r: iterable) {
                sum += r.temperature;
                count ++;
            }

            Double avg = sum / count;
            SensorReading result = new SensorReading();
            result.id = s;
            result.timestamp = timeWindow.getEnd();
            result.temperature = avg;
            collector.collect(result);

        }
    }

}

