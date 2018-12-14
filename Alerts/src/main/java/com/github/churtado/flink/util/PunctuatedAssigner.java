package com.github.churtado.flink.util;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

// this updates the watermark every time you get a reading from sensor 1
// but it could be anything

public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {

    Long bound = 60L * 1000; // 1 minute bound

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(SensorReading sensorReading, long extractedTs) {
        if (sensorReading.id == "sensor_1") {
            return new Watermark(extractedTs - bound);
        } else {
            return null;
        }
    }

    @Override
    public long extractTimestamp(SensorReading sensorReading, long l) {
        return sensorReading.timestamp;
    }
}
