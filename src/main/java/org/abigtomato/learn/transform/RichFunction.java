package org.abigtomato.learn.transform;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 富函数 RichMapFunction
 *
 * @author abigtomato
 */
@Slf4j
public class RichFunction extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(4);

        DataStream<Tuple2<String, Integer>> richMapStream = dataStream.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = -7791561601522528623L;

            @Override
            public Tuple2<String, Integer> map(SensorReading sensorReading) {
                return new Tuple2<>(sensorReading.getId(), this.getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void open(Configuration parameters) {
                // 初始化工作，如定义状态或建立连接
                log.debug("-----> open.");
            }

            @Override
            public void close() {
                // 收尾工作，如关闭连接和清空状态
                log.debug("-----> execute.");
            }
        });
        richMapStream.print("richMapStream");

        execute();
    }
}
