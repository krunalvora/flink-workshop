package com.github.krunalvora.streamingwithflink;

import com.github.krunalvora.streamingwithflink.util.IntegerSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DemoIntegerJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Integer> integerDataStream = env
          .addSource(new IntegerSource());

    // Printing original input stream
    System.out.println(integerDataStream.print());

    DataStream<Integer> outputIntDataStream = integerDataStream
          // .map(r-> r + 100);
          .map(new IncrementByHundred()); // or using a MapFunction

    // Printing output stream
    System.out.println(outputIntDataStream.print());

    env.execute();
  }

  public static class IncrementByHundred implements MapFunction<Integer, Integer> {

    @Override
    public Integer map(Integer value) throws Exception {
      return value + 100;
    }
  }



}
