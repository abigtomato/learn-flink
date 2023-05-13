package org.abigtomato.learn.table;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author abigtomato
 */
@Slf4j
public class CommonApi extends BaseContext {

    public static void main(String[] args) {
        // 直接创建TableEnv
        TableEnvironment.create(EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build());

        initTextData(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        // 创建临时表
        tableEnv.createTemporaryTable("sourceTable", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .column("temperature", DataTypes.DOUBLE())
                        .build())
                .option("path", "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\sensor.csv")
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", ",")
                        .build())
                .build());

        // 加载表
        Table sourceTable = tableEnv.from("sourceTable");
        sourceTable.printSchema();

        // 基础操作
        Table resultTable = sourceTable.select($("id"), $("timestamp"), $("temperature"))
                .filter($("id").isEqual("sensor_6"));
        log.info(resultTable.explain());
        resultTable.execute().print();
        tableEnv.toDataStream(resultTable, SensorReading.class).print();

        // 聚合统计
        Table aggTable = sourceTable.groupBy($("id"))
                .select($("id"), $("id").count().as("count"),
                        $("temperature").avg().as("avgTemperature"));
        CloseableIterator<Row> closeableIterator = aggTable.execute().collect();
        while (closeableIterator.hasNext()) {
            Row row = closeableIterator.next();
            log.info("-----> row: {}", row);
        }

        // 输出结果
        tableEnv.createTemporaryTable("outputTable", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("count", DataTypes.BIGINT())
                        .column("avgTemperature", DataTypes.DOUBLE())
                        .build())
                .option("path", "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\sensor_out")
                .build());
        resultTable.executeInsert("outputTable");

        execute();
    }
}
