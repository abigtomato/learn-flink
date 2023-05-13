package org.abigtomato.learn.table;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * TableApi & FlinkSQL
 *
 * @author abigtomato
 */
@Slf4j
public class Example extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(1);

        // 根据流创建TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        // 通过流创建Table
        Table dataTable = tableEnv.fromDataStream(dataStream, Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("temperature", DataTypes.DOUBLE())
                .build());

        // Table API
        Table resultTable = dataTable
                .select($("id"), $("timestamp"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        // Flink SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "SELECT id, `timestamp`, temperature FROM sensor WHERE id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toDataStream(resultTable, SensorReading.class).print("tableApi");
        tableEnv.toChangelogStream(resultSqlTable).print("sql");

        execute();
    }
}
