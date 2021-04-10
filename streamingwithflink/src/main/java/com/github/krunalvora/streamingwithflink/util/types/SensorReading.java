package com.github.krunalvora.streamingwithflink.util.types;

public class SensorReading {

  public String id;
  public long timestamp;
  public double temperature;

  // Empty default constructor to satisfy Flink's POJO requirements
  public SensorReading() {
  }

  public SensorReading(String id, long timestamp, double temperature) {
    this.id = id;
    this.timestamp = timestamp;
    this.temperature = temperature;
  }

  public String toString() {
    return(String.format("(%s, %d, %f)", id, timestamp, temperature));
  }
}
