package com.github.churtado.flink.exercises;

import com.github.churtado.flink.util.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
        private transient ValueState<Double> lastTemperature;
        private transient ValueState<Long> currentTimer;

        @Override
        public void open(Configuration config) throws Exception {

            // hold temperature of last sensor reading
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemperature", TypeInformation.of(new TypeHint<Double>() {})));

            // hold timestamp of currently active timer
            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", TypeInformation.of(new TypeHint<Long>() {})));

        }

        @Override
        public void processElement(SensorReading input, Context context, Collector<String> collector) throws Exception {

            Double prevTemperature = 0.0;
            if (lastTemperature.value() != null) {
                prevTemperature = lastTemperature.value();
            }
            lastTemperature.update(input.temperature);

            // get previous temperature
            if(prevTemperature == 0.0 || input.temperature < prevTemperature) {
                // temperature decreased. Invalidate current timer
                currentTimer.update(0L);
                // collector.collect("temperature decreased from " + prevTemperature + " to " + input.temperature);
            } else if (input.temperature > prevTemperature && currentTimer.value() == 0) {
                // temperature increased and we have not set a timer yet
                // set processing time timer for now + 1 second
                Long timerTs = context.timerService().currentProcessingTime() +1 ;
                context.timerService().registerProcessingTimeTimer(timerTs);
                // remember current timer
                currentTimer.update(timerTs);
                // collector.collect("set a timer because temperature increased once");
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> out) throws Exception {
            if(timestamp == currentTimer.value()) {
                out.collect("Temperature of sensor " + context.getCurrentKey() + " monotonically increased for 1 second");
                currentTimer.update(0L);
            }
        }
    }
}
