package com.github.churtado.flink.com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

        celsius
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading reading) throws Exception {
                        return reading.id;
                    }
                }).timeWindow(Time.seconds(5))
                .apply(new AverageFunction())
                .returns(SensorReading.class) // returning type hints
                .map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                        return new Tuple3<String, Double, Long>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
                    }
                })
                .returns(new TypeHint<Tuple3<String, Double, Long>>() { // returning type hints
                    @Override
                    public TypeInformation<Tuple3<String, Double, Long>> getTypeInfo() {
                        return super.getTypeInfo();
                    }
                })
                .print();

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

