package org.abigtomato.learn.window;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Optional;

/**
 * 时间窗口
 *
 * @author abigtomato
 */
@Slf4j
public class TimeWindowExample extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        // 滚动时间窗口
        DataStream<Integer> aggregateStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                .process(new ProcessWindowFunction<SensorReading, Integer, String, TimeWindow>() {
//                })
//                .apply(new WindowFunction<SensorReading, Integer, String, TimeWindow>() {
//                })
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    private static final long serialVersionUID = -140804218194759247L;

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer accumulatorLeft, Integer accumulatorRight) {
                        return accumulatorLeft + accumulatorRight;
                    }
                });
        aggregateStream.print("aggregateStream");

        // 滚动全时间窗口
        DataStream<Double> processStream = keyedStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(15)))
//                .aggregate(new AggregateFunction<SensorReading, Double, Double>() {
//                })
//                .apply(new AllWindowFunction<SensorReading, Double, TimeWindow>() {
//                })
                .process(new ProcessAllWindowFunction<SensorReading, Double, TimeWindow>() {

                    private static final long serialVersionUID = -342065020912861763L;

                    @Override
                    public void process(Context context, Iterable<SensorReading> iterable, Collector<Double> collector) {
                        Optional<Double> reduceOptional = IteratorUtils.toList(iterable.iterator()).stream()
                                .map(SensorReading::getTemperature)
                                .reduce(Double::sum);
                        collector.collect(reduceOptional.orElse(0.0));
                    }
                });
        processStream.print("processStream");

        // 处理乱序迟到的数据
        OutputTag<SensorReading> outputTag = new OutputTag<>("late");
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> applyStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .apply((WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>) (id, timeWindow, iterable, collector) -> {
                    Long windowEnd = timeWindow.getEnd();
                    Integer count = IteratorUtils.toList(iterable.iterator()).size();
                    collector.collect(new Tuple3<>(id, windowEnd, count));
                });
        applyStream.print("applyStream");

        applyStream.getSideOutput(outputTag).print("late");

        execute();
    }
}
