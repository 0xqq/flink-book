package com.github.churtado.flink.processfunction.util;

public class SensorReading {

    // making this a POJO

    public SensorReading(){
    }

    public String id;
    public Long timestamp;
    public Double temperature;
    public String location;
}
