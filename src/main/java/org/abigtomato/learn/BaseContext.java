package org.abigtomato.learn;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author abigtomato
 */
@Slf4j
public class BaseContext {

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;

    public static StreamExecutionEnvironment env() {
        return env;
    }

    public static StreamTableEnvironment tableEnv() {
        return tableEnv;
    }

    public static DataStream<SensorReading> initTextData() {
        return initTextData(Runtime.getRuntime().availableProcessors());
    }

    public static DataStream<SensorReading> initTextData(int parallelism) {
        env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(parallelism);
        return env.readTextFile("D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\sensor.csv").map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
    }

    public static DataStream<SensorReading> initSocketData() {
        return initSocketData(Runtime.getRuntime().availableProcessors());
    }

    public static DataStream<SensorReading> initSocketData(int parallelism) {
        env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(parallelism);
        return env.socketTextStream("localhost", 7777).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
    }

    public static void execute() {
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("error", e.getCause());
        }
    }
}
