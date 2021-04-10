package com.github.krunalvora.streamingwithflink;

import com.github.krunalvora.streamingwithflink.util.SensorReading;
import com.github.krunalvora.streamingwithflink.util.SensorSource;
import com.github.krunalvora.streamingwithflink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AverageSensorReadings {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(1000L);

    DataStream<SensorReading> sensorStream = env
          .addSource(new SensorSource())
          // assign timestamps and watermarks which are required for event time
          .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    DataStream<SensorReading> avgTempStream = sensorStream
          // convert to celsius temperature
          .map(r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0/9.0)))
          .keyBy(r -> r.id)
          .timeWindow(Time.seconds(5))
          .apply(new TemperatureAverager());

    avgTempStream.print();

    env.execute("Compute average sensor temperature");

  }


  public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

    @Override
    public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
      int cnt = 0;
      double sum = 0.0;
      for (SensorReading r: input) {
        cnt++;
        sum += r.temperature;
      }

      double avgTemp = sum / cnt;

      out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
    }
  }
}