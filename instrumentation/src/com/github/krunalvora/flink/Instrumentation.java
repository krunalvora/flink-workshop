package com.github.krunalvora.flink;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

public class Instrumentation {

  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  DataStream<String> socketData = env.socketTextStream("localhost", 9005);


  DataStream<String> dataStream = socketData.map(r -> r.toLowerCase(Locale.ROOT));

}
