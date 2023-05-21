package org.abigtomato.learn.transform;

import org.abigtomato.learn.model.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按键分区
 */
public class TransKeyByTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 通过指定键（key），可以将一条流从逻辑划分成不同的分区（partitions）
        // 这里所说的分区，其实就是并行处理的子任务，也就对应着任务槽（task slot）
        KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);

        keyedStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
