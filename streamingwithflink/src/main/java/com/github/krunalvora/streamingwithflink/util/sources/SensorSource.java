package com.github.krunalvora.streamingwithflink.util.sources;

import com.github.krunalvora.streamingwithflink.util.types.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Flink SourceFunction to generate SensorReadings with random temperature values.
 *
 * Each parallel instance of the source simulates 10 sensors which emit one sensor reading every 100 ms.
 *
 * Note: This is a simple data-generating source function that does not checkpoint its state.
 * In case of a failure, the source does not replay any data.
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

  private boolean running = true;

  @Override
  public void run(SourceContext<SensorReading> ctx) throws Exception {

    Random rand = new Random();

    // look up index of this parallel task
    int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

    // initialize sensor ids and temperatures
    String[] sensorIds = new String[10];
    double[] curFTemp = new double[10];
    for (int i = 0; i < 10; i++) {
      sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
      curFTemp[i] = 65 + (rand.nextGaussian() * 20);
    }

    while (running) {
      long curTime = Calendar.getInstance().getTimeInMillis();

      // emit SensorReadings
      for (int i = 0; i < 10; i++) {
        // update current temperature
        curFTemp[i] += rand.nextGaussian() * 0.5;
        // emit reading
        ctx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
      }

      Thread.sleep(500);

    }

  }

  @Override
  public void cancel() {
    this.running = false;
  }
}
