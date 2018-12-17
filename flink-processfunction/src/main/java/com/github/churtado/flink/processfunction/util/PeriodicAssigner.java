package com.github.churtado.flink.processfunction.util;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PeriodicAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

    Long bound = 60L * 1000L;       // 1 min in ms
    Long maxTs = Long.MIN_VALUE;    // the maximum observed timestamp

    @Nullable
    @Override
    // get the max value minus one minute
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }

    // return the timestamp
    @Override
    public long extractTimestamp(SensorReading sensorReading, long l) {
        maxTs = Long.max(sensorReading.timestamp, maxTs);

        return sensorReading.timestamp;
    }
}
