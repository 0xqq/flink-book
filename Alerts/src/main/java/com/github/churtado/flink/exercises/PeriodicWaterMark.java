package com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.w3c.dom.TypeInfo;
import sun.awt.SunHints;

public class PeriodicWaterMark {

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

        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                // assign timestamps and watermarks required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5))); // you need this if using event time

        DataStream<SmokeLevel> smokeLevel = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1); // warning: no checkpointing

        // key sensors by id
        KeyedStream<SensorReading, String> keyed = readings
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                });

        DataStream<Alert> alerts = keyed
                .connect(smokeLevel.broadcast())
                .flatMap(new SmokeAlerts.RaiseAlertFlatmap());

//        readings.map(new MapFunction<SensorReading, Double>() {
//            @Override
//            public Double map(SensorReading sensorReading) throws Exception {
//                return sensorReading.temperature;
//            }
//        }).print();

//        alerts.map(new MapFunction<Alert, Tuple2<String, Long>>() {
//            @Override
//            public Tuple2<String, Long> map(Alert alert) throws Exception {
//                return new Tuple2<>(alert.message, alert.timestamp);
//            }
//        }).print();

        // using a process function to emit warnings if temp monotonically increases within a second
        KeyedStream<SensorReading, String> keyedReadings = readings.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.id;
            }
        });

        keyedReadings
                .process(new TempIncreaseAlertFunction())
                .print();

        env.execute();
    }

    static class TempIncreaseAlertFunction extends  KeyedProcessFunction<String, SensorReading, String> {

        // hold temperature of last sensor reading
        private transient ValueState<SensorReading> lastReading;

        private transient ValueState<Double> lastTemp;

        @Override
        public void open(Configuration config) throws Exception {

            ValueStateDescriptor<SensorReading> descriptor =
                    new ValueStateDescriptor<>(
                            "lastReading", // the state name
                            TypeInformation.of(new TypeHint<SensorReading>() {}) // type information
                    );

            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Types.DOUBLE));

            // hold temperature of last sensor reading
            lastReading = getRuntimeContext()
                    .getState(descriptor);

            // hold timestamp of currently active timer
//            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Types.LONG));

        }

        @Override
        public void processElement(SensorReading input, Context context, Collector<String> collector) throws Exception {
            SensorReading prevReading = null;

            if(lastReading.value() != null) {
                prevReading = lastReading.value(); // assign it to use it below
                lastReading.update(input); // update in case it's empty for next round
            }

            // get previous temperature
            if(prevReading.temperature == 0.0 || input.temperature < prevReading.temperature) {
                // temperature decreased. Invalidate current timer
//                currentTimer.update(0L);
            } else if (input.temperature > prevReading.temperature /*&& currentTimer.value() == 0*/) {
//                // temperature increased and we have not set a timer yet
                // set processing time timer for now + 1 second
//                Long timerTs = context.timerService().currentProcessingTime() +1 ;
//                context.timerService().registerEventTimeTimer(timerTs);
                // remember current timer
//                currentTimer.update(timerTs);
//            }

            collector.collect("stuff is going on");
        }
    }
}
