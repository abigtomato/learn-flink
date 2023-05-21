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
 * 有界流处理WordCount
 */
public class BoundedStreamWordCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((FlatMapFunction<String, String>) (line, words) -> Arrays.stream(line.split(" ")).forEach(words::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 分组，传入一个匿名函数作为键选择器
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        result.print();

        try {
            // 开始执行任务
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
