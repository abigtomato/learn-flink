package org.abigtomato.learn.state;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

/**
 * 键控状态示例
 *
 * @author abigtomato
 */
public class KeyedStateApplicationCase extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        // 检测温度跳变，前后两次跳变超过阈值，输出告警信息
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .flatMap(new RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>() {

                    private static final long serialVersionUID = -8456789330114736775L;

                    private ValueState<Double> lastTemperatureState;

                    @Override
                    public void open(Configuration parameters) {
                        this.lastTemperatureState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("lastTemperatureState", Double.class));
                    }

                    @Override
                    public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
                        final double threshold = 10.0;
                        Double lastTemperature = this.lastTemperatureState.value();
                        if (lastTemperature != null) {
                            double diff = Math.abs(sensorReading.getTemperature() - lastTemperature);
                            if (diff >= threshold) {
                                out.collect(new Tuple3<>(sensorReading.getId(), lastTemperature, sensorReading.getTemperature()));
                            }
                        }
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
