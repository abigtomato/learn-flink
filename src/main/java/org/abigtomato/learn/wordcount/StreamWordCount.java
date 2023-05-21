package org.abigtomato.learn.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 流处理WordCount
 */
public class StreamWordCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDSS = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOns = lineDSS
                .flatMap((FlatMapFunction<String, String>) (line, words) -> Arrays.stream(line.split(" ")).forEach(words::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOns.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
