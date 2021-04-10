package com.github.krunalvora.streamingwithflink;

import com.github.krunalvora.streamingwithflink.util.sources.IntegerSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
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
          Tuple3.of(1, 1, 1), Tuple3.of(1, 1, 2), Tuple3.of(2, 1, 2), Tuple3.of(1, 1, 3));

    DataStream<Tuple3<Integer, Integer, Integer>> outputIntDataStream = integerTupleStream
          .keyBy(0)
          // .sum(1);
          // .max(2);
          .reduce(new SumReduce());

    // Printing output stream
    System.out.println(outputIntDataStream.print());

    env.execute();
  }

  public static void executeMultiStreamTransformations() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Data source stream from elements of a list
    DataStream<Integer> integerStreamEven = env.fromElements(2, 4, 6, 8);
    DataStream<Integer> integerStreamOdd = env.fromElements(1, 3, 5, 7, 9);
    DataStream<String> stringStreamOdd = env.fromElements("1", "3", "5", "7", "9");

    // DataStream<Integer> outputStream = integerStreamOdd.union(integerStreamEven);
    DataStream<Integer> outputStream = integerStreamOdd
          .connect(stringStreamOdd)  // creates ConnectedStreams<Integer, String>
          .map(new allInts());

    System.out.println(outputStream.print());

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


  public static class SumReduce implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
    @Override
    public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1,
                                                    Tuple3<Integer, Integer, Integer> value2) throws Exception {
      return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2);
    }
  }

  public static class allInts implements CoMapFunction<Integer, String, Integer> {

    @Override
    public Integer map1(Integer value) throws Exception {
      return value;
    }

    @Override
    public Integer map2(String value) throws Exception {
      return Integer.parseInt(value);
    }
  }


  public static void main(String[] args) throws Exception {
    // executeBasicTransformations();
    // executeKeyedTransformations();
    executeMultiStreamTransformations();
  }
}
