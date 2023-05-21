package org.abigtomato.learn.transform;

import org.abigtomato.learn.model.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 扁平映射
 */
public class TransFlatmapTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 将数据流中的整体（一般是集合类型）拆分成一个一个的个体使用
        // 消费一个元素，可以产生 0 到多个元素
        SingleOutputStreamOperator<String> operator = stream.flatMap((FlatMapFunction<Event, String>) (event, collector) -> {
            if (event.user.equals("Mary")) {
                collector.collect(event.user);
            } else if (event.user.equals("Bob")) {
                collector.collect(event.user);
                collector.collect(event.path);
            }
        });

        operator.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
