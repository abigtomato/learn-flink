package org.abigtomato.learn.transform;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 重分区 partitionCustom、shuffle、rescale、broadcast、global、startNewChain、disableChaining、slotSharingGroup
 *
 * @author abigtomato
 */
@Slf4j
public class Partition extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(4);

        // 自定义分区
        DataStream<SensorReading> partitionCustomStream = dataStream.partitionCustom(new Partitioner<Integer>() {

            private static final long serialVersionUID = -1709444898257558296L;

            @Override
            public int partition(Integer integer, int i) {
                return 2;
            }
        }, new KeySelector<SensorReading, Integer>() {

            private static final long serialVersionUID = -4020842081647159338L;

            @Override
            public Integer getKey(SensorReading sensorReading) {
                return 1;
            }
        });
        partitionCustomStream.print("partitionCustomStream");

        // 随机分区
        DataStream<SensorReading> shuffleStream = dataStream.shuffle();
        shuffleStream.print("shuffleStream");

        DataStream<SensorReading> rescaleStream = dataStream.rescale();
        rescaleStream.print("rescaleStream");

        // 广播
        DataStream<SensorReading> broadcastStream = dataStream.broadcast();
        broadcastStream.print("broadcastStream");

        DataStream<SensorReading> globalStream = dataStream.global();
        globalStream.print("globalStream");

        dataStream.map(line -> line + "-startNewChain").startNewChain().map(line -> line);

        dataStream.map(line -> line + "-disableChaining").disableChaining();

        dataStream.map(line -> line + "-slotSharingGroup").slotSharingGroup("test");

        execute();
    }
}
