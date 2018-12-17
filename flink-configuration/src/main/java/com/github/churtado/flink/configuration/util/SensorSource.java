package com.github.churtado.flink.configuration.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private volatile boolean running = true;
    private static final int numSensors = 10;

    @Override
    public void run(final SourceContext<SensorReading> sourceContext) throws Exception {

        // initialize random number generator
        final Random rand = new Random();

        // I'm guessing this is to pool all 10 sensors under 2 threads
        final ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);

        try{
            while (running) {
                for (int i = 0; i < numSensors; i++) {

                    // update the timestamp
                    long cur = System.currentTimeMillis();

                    // update the sensor value
                    Double reading = ThreadLocalRandom.current().nextDouble(30.0, 105.0);

                    // sensor id
                    String id = "sensor_" + i;
                    SensorReading event = new SensorReading();
                    event.id = id;
                    event.timestamp = Calendar.getInstance().getTimeInMillis();
                    event.temperature = reading;

                    exec.schedule(() -> {
                        sourceContext.collect(event);
                    }, 600, TimeUnit.MILLISECONDS);
                }
                Thread.sleep(500);
            }
        } finally {
            exec.shutdownNow();
        }


    }

    @Override
    public void cancel() {
        running = false;
    }
}

