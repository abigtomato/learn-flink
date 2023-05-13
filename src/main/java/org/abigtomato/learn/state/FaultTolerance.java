package org.abigtomato.learn.state;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 状态后端
 *
 * @author abigtomato
 */
public class FaultTolerance extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        // HashMap状态后端会根据CheckpointStorage的配置将工作状态保存在TaskManager和检查点的JVM 堆内存中
        env().setStateBackend(new HashMapStateBackend());

        // 开启检查点并设置间隔
        env().enableCheckpointing(300);

        // 设置检查点存储模式
        // JobManagerCheckpointStorage：作业管理器存储
        // FileSystemCheckpointStorage：文件系统存储
        env().getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env().getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://checkpoint"));

        // 检查点高级设置
        // setCheckpointingMode检查点模式：EXACTLY_ONCE所有任务恰好执行一次
        // setCheckpointTimeout设置检查点在被丢弃之前可能花费的最长时间
        // setMaxConcurrentCheckpoints：最大并发检查点尝试次数
        // setMinPauseBetweenCheckpoints：设置检查点尝试之间的最小暂停
        // setTolerableCheckpointFailureNumber：定义了在整个作业故障转移之前将容忍多少次连续检查点故障
        env().getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env().getCheckpointConfig().setCheckpointTimeout(60000);
        env().getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env().getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        env().getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 检查点重启策略
        // fixedDelayRestart：固定延迟重启，参数：重启尝试次数3和重试延迟10000
        // failureRateRestart：失败率重启，参数：在给定间隔Time.minutes(10)内重新启动的最大次数3，然后再失败作业，重新启动尝试之间的延迟为Time.minutes(1)
        env().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env().setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));

        dataStream.print();

        execute();
    }
}
