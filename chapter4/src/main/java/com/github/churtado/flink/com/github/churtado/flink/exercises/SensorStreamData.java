package com.github.churtado.flink.com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.SensorReading;
import com.github.churtado.flink.util.SensorSource;
import com.github.churtado.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
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

        DataStream<Tuple3<Double, Long, String>> readings = env
                .addSource(new SensorSource())
                .setParallelism(4) // ingest through a source
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time

        // simple dump of the stream
        /*readings
                .keyBy(2)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sum(0);*/

        // convert to celsius
        DataStream<Tuple3<Double, Long, String>> celsius = readings.map(new MapFunction<Tuple3<Double, Long, String>, Tuple3<Double, Long, String>>() {
            @Override
            public Tuple3<Double, Long, String> map(Tuple3<Double, Long, String> tuple) throws Exception {
                return new Tuple3<Double, Long, String>((tuple.f0 - 32) * (5.0 / 9.0), tuple.f1, tuple.f2);
            }
        });

        celsius
                .keyBy(new KeySelector<Tuple3<Double, Long, String>, String>() {
                    @Override
                    public String getKey(Tuple3<Double, Long, String> tuple) throws Exception {
                        return tuple.f2;
                    }
                }).timeWindow(Time.seconds(5))
                .apply(new AverageFunction());


        celsius.print();

        env.execute();

    }

    static class AverageFunction implements WindowFunction<Tuple3<Double, Long, String>, Tuple3<Double, Long, String>, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Tuple3<Double, Long, String>> iterable, Collector<Tuple3<Double, Long, String>> collector) throws Exception {

            Double sum = 0.0;
            Double count = 0.0;

            for(Tuple3<Double, Long, String> t: iterable) {
                sum += t.f0;
                count ++;
            }

            Double avg = sum / count;
            Tuple3<Double, Long, String> result = new Tuple3(avg, timeWindow.getEnd(), s);
            collector.collect(result);

        }
    }

}

