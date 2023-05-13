package org.abigtomato.learn.window;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 计数窗口
 *
 * @author abigtomato
 */
@Slf4j
public class CountWindow extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        // 滑动计数窗口
        DataStream<Double> aggregateStream = dataStream
                .keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    private static final long serialVersionUID = -8819303783627537170L;

                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>(accumulator.f0 + sensorReading.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> left, Tuple2<Double, Integer> right) {
                        return new Tuple2<>(left.f0 + right.f0, left.f1 + right.f1);
                    }
                });
        aggregateStream.print("aggregateStream");

        execute();
    }
}
