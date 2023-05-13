package org.abigtomato.learn.state;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 算子状态
 *
 * @author abigtomato
 */
public class OperatorState extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        dataStream.map(new MyMapFunction()).print();

        execute();
    }

    private static class MyMapFunction implements MapFunction<SensorReading, Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 892694879021166031L;

        private AtomicInteger count;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return this.count.decrementAndGet();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            this.count = new AtomicInteger(0);
        }
    }
}
