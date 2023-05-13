package org.abigtomato.learn.example.analysis;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.StringJoiner;
import java.util.regex.Pattern;

/**
 * @author abigtomato
 */
public class HotPages {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init(1);

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\apache.csv";
        DataStream<String> inputDataStream = context.getStreamDataFromText(path);

        DataStream<ApacheLogEvent> apacheLogEventDataStream = inputDataStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((SerializableTimestampAssigner<ApacheLogEvent>)
                        (apacheLogEvent, recordTimestamp) -> apacheLogEvent.getTimestamp()));

        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
            private static final long serialVersionUID = 1056643360018992741L;
        };
        SingleOutputStreamOperator<PageViewCount> pageViewCountDataStream = apacheLogEventDataStream
                .filter(data -> "GET".equals(data.getMethod())
                        && Pattern.matches("^((?!\\.(css|js|png|ico)$).)*$", data.getUrl()))
                .keyBy(ApacheLogEvent::getUrl, TypeInformation.of(String.class))
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new AggregateFunction<ApacheLogEvent, Long, Long>() {

                    private static final long serialVersionUID = 2389388757522411397L;

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(ApacheLogEvent value, Long accumulator) {
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
                }, new WindowFunction<Long, PageViewCount, String, TimeWindow>() {

                    private static final long serialVersionUID = -4170825694301555778L;

                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) {
                        out.collect(new PageViewCount(key, window.getEnd(), input.iterator().next()));
                    }
                });

        pageViewCountDataStream.getSideOutput(lateTag).print("late");

        DataStream<Object> resultDataStream = pageViewCountDataStream
                .keyBy(PageViewCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, PageViewCount, Object>() {

                    private static final long serialVersionUID = -1719122745893615941L;

                    private ValueState<BoundedPriorityQueue> valueState;

                    @Override
                    public void open(Configuration parameters) {
                        this.valueState = this.getRuntimeContext().getState(new ValueStateDescriptor<>("topN",
                                TypeInformation.of(BoundedPriorityQueue.class)));
                    }

                    @Override
                    public void processElement(PageViewCount pageViewCount, Context ctx, Collector<Object> out) {
                        try {
                            BoundedPriorityQueue<PageViewCount> boundedPriorityQueue = this.valueState.value();
                            if (boundedPriorityQueue == null) {
                                boundedPriorityQueue = new BoundedPriorityQueue<>(10,
                                        (left, right) -> right.getCount().intValue() - left.getCount().intValue());
                            }
                            boundedPriorityQueue.add(pageViewCount);
                            this.valueState.update(boundedPriorityQueue);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
                        ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 60 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) {
                        if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                            this.valueState.clear();
                            return;
                        }

                        try {
                            BoundedPriorityQueue<PageViewCount> bpq = this.valueState.value();
                            StringJoiner stringJoiner = new StringJoiner(", ", "[", "]");
                            for (PageViewCount pvc = bpq.get(); pvc != null; pvc = bpq.get()) {
                                stringJoiner.add(pvc.getUrl() + "-" + (pvc.getCount() != null ? pvc.getCount() : ""));
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
