package org.abigtomato.learn.transform;

import org.abigtomato.learn.model.Event;
import org.abigtomato.learn.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 规约聚合
 */
public class TransReduceTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 对已有的数据进行归约处理，把每一个新输入的数据和当前已经归约出来的值，再做一个聚合计算
        // 2. 对于一组数据，我们可以先取两个进行合并，然后再将合并的结果看作一个数据、再跟后面的数据合并
        // 3. 流处理的底层实现过程中，实际上是将中间“合并的结果”作为任务的一个状态保存起来的；之后每来一个新的数据，就和之前的聚合状态进一步做归约
        // 4. ReduceFunction 内部会维护一个初始值为空的累加器，注意累加器的类型和输入元素的类型相同
        //  4.1. 当第一条元素到来时，累加器的值更新为第一条元素的值，当新的元素到来时
        //  4.2. 新元素会和累加器进行累加操作，这里的累加操作就是 reduce 函数定义的运算规则。然后将更新以后的累加器的值向下游输出

        // 按照用户 id 进行分区，然后用一个 reduce 算子实现 sum 的功能
        // 统计每个用户访问的频次；进而将所有统计结果分到一组
        // 用另一个 reduce 算子实现 maxBy 的功能，记录所有用户中访问频次最高的那个
        env.addSource(new ClickSource()).map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 = value2.f1);
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean getKey(Tuple2<String, Long> tuple2) throws Exception {
                return true;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
