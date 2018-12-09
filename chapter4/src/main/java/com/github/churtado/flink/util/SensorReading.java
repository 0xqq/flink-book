package com.github.churtado.flink.util;

import java.util.Calendar;

public class SensorReading {

    public SensorReading(String id, Double temperature){
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
        timestamp = Calendar.getInstance().getTimeInMillis();
    }

    String id;
    Long timestamp;
    Double temperature;
}
