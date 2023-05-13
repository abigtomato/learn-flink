package org.abigtomato.learn.window;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事件时间窗口
 *
 * @author abigtomto
 */
@Slf4j
public class EventTimeWindow extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData();
        env().setParallelism(1);
        env().getConfig().setAutoWatermarkInterval(100);

        DataStream<SensorReading> timeDataStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {

                    private static final long serialVersionUID = -8971339563488477201L;

                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        KeyedStream<SensorReading, String> keyedStream = timeDataStream.keyBy(new KeySelector<SensorReading, String>() {

            private static final long serialVersionUID = -229212054772334829L;

            @Override
            public String getKey(SensorReading sensorReading) {
                return sensorReading.getId();
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<>("late");
        SingleOutputStreamOperator<Integer> aggregateStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new RichAggregateFunction<SensorReading, AtomicInteger, Integer>() {

                    private static final long serialVersionUID = -2958128651202668544L;

                    @Override
                    public AtomicInteger createAccumulator() {
                        return new AtomicInteger(0);
                    }

                    @Override
                    public AtomicInteger add(SensorReading sensorReading, AtomicInteger atomicInteger) {
                        atomicInteger.accumulateAndGet(1, Integer::sum);
                        return atomicInteger;
                    }

                    @Override
                    public Integer getResult(AtomicInteger atomicInteger) {
                        return atomicInteger.get();
                    }

                    @Override
                    public AtomicInteger merge(AtomicInteger left, AtomicInteger right) {
                        left.addAndGet(right.get());
                        return left;
                    }
                });

        aggregateStream.print("aggregateStream");
        aggregateStream.getSideOutput(outputTag).print("late");

        execute();
//        .window(new WindowAssigner<SensorReading, Window>() {
//
//            private static final long serialVersionUID = 3880844711727624483L;
//
//            @Override
//            public Collection<Window> assignWindows(SensorReading element, long timestamp, WindowAssignerContext context) {
//                return null;
//            }
//
//            @Override
//            public Trigger<SensorReading, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
//                return null;
//            }
//
//            @Override
//            public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
//                return null;
//            }
//
//            @Override
//            public boolean isEventTime() {
//                return false;
//            }
//        });

    }
}
