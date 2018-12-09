package com.github.churtado.flink.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SensorSource extends RichParallelSourceFunction<Tuple3<Double, Long, String>> {

    private volatile boolean running = true;
    private static final int numSensors = 10;

    @Override
    public void run(final SourceContext<Tuple3<Double, Long, String>> sourceContext) throws Exception {

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
                    Double reading = rand.nextDouble();

                    // sensor id
                    String id = "sensor_" + i;
                    final Tuple3<Double, Long, String> event = new Tuple3(reading ,cur, id);

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
