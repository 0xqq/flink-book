package com.github.churtado.flink.configuration.util;

public class SensorReading {

    // making this a POJO

    public SensorReading(){
    }

    public String id;
    public Long timestamp;
    public Double temperature;
    public String location;
}
