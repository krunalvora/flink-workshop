package com.github.krunalvora.flink.wordcount;

import com.github.krunalvora.flink.wordcount.model.Dummy;
import com.github.krunalvora.flink.wordcount.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.iterative.event.WorkerDoneEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountUsingPOJO {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<WordWithCount> counts = env
          .socketTextStream("localhost", 9999)
          .flatMap(new WordSplitter())
          .keyBy(node -> node.getWord())
          .timeWindow(Time.seconds(3))
          .sum("count");

    counts.print();

    env.getConfig().disableGenericTypes();

    env.execute("Socket Wordcount Example");
  }

  public static class WordSplitter implements FlatMapFunction<String, WordWithCount> {
    @Override
    public void flatMap(String sentence, Collector<WordWithCount> out) throws Exception {
      for (String word: sentence.split(" ")) {
        out.collect(new WordWithCount(word, 1, new Dummy(word)));
      }
    }
  }
}
