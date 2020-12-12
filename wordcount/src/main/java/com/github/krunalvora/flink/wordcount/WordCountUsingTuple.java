package com.github.krunalvora.flink.wordcount;

import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class WordCountUsingTuple {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    List<String> words = Arrays.asList("Apache Flink", "Flink", "Apache", "Kafka");

    DataStream<Tuple2<String, Integer>> counts = env
          .socketTextStream("localhost", 9999)
          // .fromCollection(words)
          .flatMap(new Splitter())
          .keyBy(0)
          .timeWindow(Time.seconds(1))
          .sum(1);

    counts.print();

    env.getConfig().disableGenericTypes();

    env.execute("Socket Wordcount Example");
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word: sentence.split(" ")) {
        out.collect(new Tuple2<String, Integer>(word, 1));
      }
    }
  }
}
