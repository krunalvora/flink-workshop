package com.github.krunalvora.streamingwithflink.util.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class IntegerSource implements SourceFunction<Integer> {
  private boolean running = true;

  Random random = new Random();

  @Override
  public void run(SourceContext<Integer> ctx) throws Exception {
    while (running) {
      ctx.collect(random.nextInt(100));  // Generating integers from 0 to 100
      Thread.sleep(500);
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }
}
