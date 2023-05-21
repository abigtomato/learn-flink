package org.abigtomato.learn.transform;

import org.abigtomato.learn.model.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 过滤
 */
public class TransFilterTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 对数据流执行一个过滤，通过一个布尔条件表达式设置过滤条件
        // 对于每一个流内元素进行判断，若为 true 则元素正常输出，若为 false 则元素被过滤掉
        SingleOutputStreamOperator<Event> operator = stream.filter((FilterFunction<Event>) event -> event.user.equals("Mary"));

        operator.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
