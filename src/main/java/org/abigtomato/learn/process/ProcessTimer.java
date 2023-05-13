package org.abigtomato.learn.process;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * 定时器
 *
 * @author abigtomato
 */
@Slf4j
public class ProcessTimer extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<String, SensorReading, Integer>() {

                    private static final long serialVersionUID = 7213271851726502726L;

                    // 定时器状态
                    private ValueState<Long> tsTimerState;

                    @Override
                    public void open(Configuration parameters) {
                        this.tsTimerState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("tsTimerState", Long.class));
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws IOException {
                        Long timestamp = ctx.timestamp();
                        log.info("-----> 当前时间戳: {}", timestamp);

                        String currentKey = ctx.getCurrentKey();
                        log.info("-----> 当前键值: {}", currentKey);

                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        log.info("-----> 当前处理时间: {}", currentProcessingTime);

                        long currentWatermark = ctx.timerService().currentWatermark();
                        log.info("-----> 当前水位线: {}", currentWatermark);

                        // 注册处理时间定时器，会在指定时间触发
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000);
                        // 注册事件事件定时器
                        ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 10);

                        // 更新定时器状态
                        this.tsTimerState.update(currentProcessingTime);

                        out.collect(value.getId().length());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) {
                        log.info("-----> 触发 {} 定时器", timestamp);
                        log.info("-----> currentKey: {}, timeDomain: {}", ctx.getCurrentKey(), ctx.timeDomain());
                    }

                    @Override
                    public void close() {
                        this.tsTimerState.clear();
                    }
                });
        resultStream.print();

        execute();
    }
}
