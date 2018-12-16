package com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.SensorReading;
import com.github.churtado.flink.util.SensorSource;
import com.github.churtado.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CoProcess {

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

        DataStream<Tuple2<String, Long>> filterSwitches = env.fromElements(
            new Tuple2<String, Long>("sensor_2", 10*1000L), // forward sensor_2 for 10 seconds
            new Tuple2<String, Long>("sensor_7", 20*1000L)  // forward sensor_7 for 20 seconds)
        );

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time

        ConnectedStreams<SensorReading, Tuple2<String, Long>> connectedStream =
                readings.connect(filterSwitches);

        DataStream<SensorReading> forwardedReadings = connectedStream
                .keyBy(new KeySelector<SensorReading, Object>() {
                    @Override
                    public Object getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                }, new KeySelector<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return stringLongTuple2.f0;
                    }
                })
            .process(new ReadingFilter());

        readings.map(new MapFunction<SensorReading, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.id, sensorReading.temperature, sensorReading.timestamp);
            }
        }).print();

        env.execute();
    }

    static class ReadingFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        // switch to enable forwarding
        private transient ValueState<Boolean> forwardingEnabled;

        // hold timestamp of currently active disable timer
        private transient ValueState<Long> disableTimer;

        @Override
        public void open(Configuration config) throws Exception {

            forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("filterSwitch", Types.BOOLEAN));
            disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
        }

        @Override
        public void processElement1(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
            if(forwardingEnabled.value()) {
                collector.collect(sensorReading);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> switchTuple, Context context, Collector<SensorReading> collector) throws Exception {
            // enable reading forwarding
            forwardingEnabled.update(true);

            // set disable forward timer
            Long timerTimestamp = context.timerService().currentProcessingTime() + switchTuple.f1;
            context.timerService().registerProcessingTimeTimer(timerTimestamp);
            disableTimer.update(timerTimestamp);
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading>.OnTimerContext context , Collector<SensorReading> out) throws Exception {
            if(timestamp == disableTimer.value()) {
                // remove all state. Forward switch will be false by default
                forwardingEnabled.clear();
                disableTimer.clear();
            }
        }
    }

}
