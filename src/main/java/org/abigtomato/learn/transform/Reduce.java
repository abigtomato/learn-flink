package org.abigtomato.learn.transform;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 归约算子 reduce
 *
 * @author abigtomato
 */
@Slf4j
public class Reduce extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(1);

        DataStream<SensorReading> reduceStream = dataStream.keyBy(SensorReading::getId).reduce(new ReduceFunction<SensorReading>() {

            private static final long serialVersionUID = 5075644027770995121L;

            @Override
            public SensorReading reduce(SensorReading v1, SensorReading v2) {
                return SensorReading.builder()
                        .id(v1.getId())
                        .timestamp(v2.getTimestamp())
                        .temperature(Math.max(v1.getTemperature(), v2.getTemperature()))
                        .build();
            }
        });
        reduceStream.print("reduceStream");

        execute();
    }
}
