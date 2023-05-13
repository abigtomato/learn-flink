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
 * 检测一段时间内的温度连续上升，输出报警
 *
 * @author abigtomato
 */
@Slf4j
public class ProcessTimerApplicationCase extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        SingleOutputStreamOperator<String> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {

                    private static final long serialVersionUID = -7073668999205265280L;

                    // 保存上一次的温度值
                    private ValueState<Double> lastTemperatureState;
                    // 定时器状态 存储定时器触发时间 时间戳就是key
                    private ValueState<Long> timerTsState;

                    @Override
                    public void open(Configuration parameters) {
                        this.lastTemperatureState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("lastTempState", Double.class));
                        this.timerTsState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("timerTsState", Long.class));
                    }

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws IOException {
                        Double lastTemperature = this.lastTemperatureState.value();
                        Long timerTs = this.timerTsState.value();

                        Double currentTemperature = value.getTemperature();
                        Long timestamp = value.getTimestamp();
                        if (currentTemperature > lastTemperature && timerTs == null) {
                            // 温度上升且没有定时器，则初始化
                            long ts = timestamp + 10 * 1000;
                            ctx.timerService().registerEventTimeTimer(ts);
                            this.timerTsState.update(ts);
                        } else if (value.getTemperature() < lastTemperature && timerTs != null) {
                            // 温度下降且定时器存在，则清除定时器
                            ctx.timerService().deleteEventTimeTimer(timerTs);
                            this.timerTsState.clear();
                        }

                        this.lastTemperatureState.update(value.getTemperature());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        out.collect(String.format("传感器 %s 温度值连续 %d 秒上升.", ctx.getCurrentKey(), 10));
                        this.timerTsState.clear();
                    }

                    @Override
                    public void close() {
                        this.lastTemperatureState.clear();
                    }
                });
        resultStream.print();

        execute();
    }
}
