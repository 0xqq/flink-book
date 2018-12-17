package com.github.churtado.flink.configuration.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SmokeLevelSource extends RichParallelSourceFunction<SmokeLevel> {

    private volatile boolean running = true;
    private static final int numSensors = 10;

    @Override
    public void run(final SourceContext<SmokeLevel> sourceContext) throws Exception {

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
                    Double reading = rand.nextDouble() * 10;

                    // sensor id
                    String id = "sensor_" + i;
                    SmokeLevel event = SmokeLevel.Low;

                    if(reading > 3 && reading <= 6) {
                        event = SmokeLevel.Medium;
                    }

                    if(reading > 6) {
                        event = SmokeLevel.High;
                    }

                    SmokeLevel finalEvent = event;
                    exec.schedule(() -> {
                        sourceContext.collect(finalEvent);
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

