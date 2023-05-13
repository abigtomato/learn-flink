package org.abigtomato.learn.example.analysis;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Random;

/**
 * @author abigtomato
 */
public class PageView {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init();

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\UserBehavior.csv";
        DataStream<String> inputDataStream = context.getStreamDataFromText(path);

        DataStream<UserBehavior> userBehaviorDataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]),
                    new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>)
                        (userBehavior, recordTimestamp) -> userBehavior.getTimestamp() * 1000L));

        DataStream<PageViewCount> aggDataStream = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {

                    private static final long serialVersionUID = -7318875767662173797L;

                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                }).keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<Tuple2<Integer, Long>, Long, Long>() {

                    private static final long serialVersionUID = -7428212975634694985L;

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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
                }, new WindowFunction<Long, PageViewCount, Integer, TimeWindow>() {

                    private static final long serialVersionUID = -6706632779318011348L;

                    @Override
                    public void apply(Integer integer, TimeWindow window,
                                      Iterable<Long> input, Collector<PageViewCount> out) {
                        out.collect(new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()));
                    }
                });

        DataStream<PageViewCount> resultDataStream = aggDataStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, PageViewCount, PageViewCount>() {

                    private static final long serialVersionUID = 6571688489742971688L;

                    private ValueState<Long> totalCountState;

                    @Override
                    public void open(Configuration parameters) {
                        this.totalCountState = this.getRuntimeContext().getState(
                                new ValueStateDescriptor<>("total-count", TypeInformation.of(Long.class)));
                    }

                    @Override
                    public void processElement(PageViewCount pageViewCount, Context ctx, Collector<PageViewCount> out) {
                        try {
                            if (this.totalCountState.value() == null) {
                                this.totalCountState.update(0L);
                            }
                            this.totalCountState.update(this.totalCountState.value() + pageViewCount.getCount());
                            ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) {
                        try {
                            Long totalCount = this.totalCountState.value();
                            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
                            this.totalCountState.clear();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
        resultDataStream.print();

        context.execute();
    }
}
