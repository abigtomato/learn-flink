package org.abigtomato.learn.example.analysis;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Duration;
import java.util.StringJoiner;

/**
 * 热门商品统计
 *
 * @author abigtomato
 */
public class HotItems {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init(1);

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\UserBehavior.csv";
        DataStream<String> inputStream = context.getStreamDataFromText(path);

        DataStream<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]),
                    Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {

                    private static final long serialVersionUID = -1518211498177878707L;

                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long recordTimestamp) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                }));

        DataStream<ItemViewCount> itemViewCountDataStream = userBehaviorDataStream
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .keyBy(UserBehavior::getItemId, TypeInformation.of(Long.class))
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {

                    private static final long serialVersionUID = 1815759625495465063L;

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new WindowFunction<Long, ItemViewCount, Long, TimeWindow>() {

                    private static final long serialVersionUID = 6693139513766833L;

                    @Override
                    public void apply(Long itemId, TimeWindow window, Iterable<Long> input,
                                      Collector<ItemViewCount> out) {
                        long start = window.getStart();
                        long end = window.getEnd();
                        Long count = input.iterator().next();
                        out.collect(new ItemViewCount(itemId, start, end, count));
                    }
                });

        DataStream<String> resultDataStream = itemViewCountDataStream.keyBy(new KeySelector<ItemViewCount, String>() {

            private static final long serialVersionUID = -8704828420408022860L;

            @Override
            public String getKey(ItemViewCount itemViewCount) {
                return itemViewCount.getWindowStart() + "_" + itemViewCount.getWindowEnd();
            }
        }, TypeInformation.of(String.class)).process(new KeyedProcessFunction<String, ItemViewCount, String>() {

            private static final long serialVersionUID = 494925944340044913L;

            private ValueState<BoundedPriorityQueue> valueState;

            @Override
            public void open(Configuration parameters) {
                this.valueState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("topN",
                        TypeInformation.of(BoundedPriorityQueue.class)));
            }

            @Override
            public void processElement(ItemViewCount itemViewCount, Context ctx, Collector<String> out) {
                try {
                    BoundedPriorityQueue<ItemViewCount> boundedPriorityQueue = this.valueState.value();
                    if (boundedPriorityQueue == null) {
                        boundedPriorityQueue = new BoundedPriorityQueue<>(10,
                                (left, right) -> right.getCount().intValue() - left.getCount().intValue());
                    }
                    boundedPriorityQueue.add(itemViewCount);
                    this.valueState.update(boundedPriorityQueue);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ctx.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                try {
                    String currentKey = ctx.getCurrentKey();
                    BoundedPriorityQueue<ItemViewCount> bpq = this.valueState.value();
                    StringJoiner stringJoiner = new StringJoiner(", ", "[", "]");
                    for (ItemViewCount itemViewCount = bpq.get(); itemViewCount != null; itemViewCount = bpq.get()) {
                        stringJoiner.add(currentKey + "-" + itemViewCount.getItemId() + "-" +
                                (itemViewCount.getCount() != null ? itemViewCount.getCount() : ""));
                    }
                    out.collect(stringJoiner.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        resultDataStream.print();

        context.execute();
    }
}
