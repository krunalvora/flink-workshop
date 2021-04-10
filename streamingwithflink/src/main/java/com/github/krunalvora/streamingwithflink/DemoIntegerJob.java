package com.github.krunalvora.streamingwithflink;

import com.github.krunalvora.streamingwithflink.util.sources.IntegerSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class DemoIntegerJob {

  public static void executeBasicTransformations() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Integer> integerDataStream = env
          .addSource(new IntegerSource());

    DataStream<Integer> outputIntDataStream = integerDataStream
          // .map(r -> r + 100);  // using a Lambda function
          .map(new IncrementByHundred())  // or using a MapFunction Interface
          // .filter(r -> r % 2 == 0)
          .filter(new DropOdd())
          .flatMap(new AddNeighboringIntegers());

    // Printing output stream
    System.out.println(outputIntDataStream.print());

    env.execute();
  }


  public static void executeKeyedTransformations() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Data source stream from elements of a list
    DataStream<Tuple3<Integer, Integer, Integer>> integerTupleStream = env.fromElements(
          Tuple3.of(1, 1, 4), Tuple3.of(1, 2, 3), Tuple3.of(2, 1, 2), Tuple3.of(1, 3, 4));

    DataStream<Tuple3<Integer, Integer, Integer>> outputIntDataStream = integerTupleStream
          .keyBy(0)
          .sum(1);
          // .max(2);

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


  public static class DropOdd implements FilterFunction<Integer> {
    @Override
    public boolean filter(Integer value) throws Exception {
      return value % 2 == 0;
    }
  }


  public static class AddNeighboringIntegers implements FlatMapFunction<Integer, Integer> {
    @Override
    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
      out.collect(value - 1);
      out.collect(value);
      out.collect(value + 1);
    }
  }


  public static void main(String[] args) throws Exception {
    // executeBasicTransformations();
    executeKeyedTransformations();
  }
}
