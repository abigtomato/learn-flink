package org.abigtomato.learn.transform;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * 滚动聚合算子 keyBy
 *
 * @author abigtomato
 */
@Slf4j
public class RollingAggregation extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(4);

        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        DataStream<SensorReading> maxByStream = keyedStream.maxBy("temperature");
        maxByStream.print("maxByStream");

        execute();
    }
}
