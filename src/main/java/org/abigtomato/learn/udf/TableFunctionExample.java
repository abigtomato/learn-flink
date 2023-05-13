package org.abigtomato.learn.udf;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

/**
 * 自定义表函数
 *
 * @author abigtomato
 */
public class TableFunctionExample extends BaseContext {

    public static void main(String[] args) throws Exception {
        DataStream<SensorReading> dataStream = initTextData(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 自定义表函数，实现将id拆分，并输出（word, length）
        // table API
        tableEnv.createTemporarySystemFunction("split", new Split("_"));
        Table resultTable = sensorTable
                .joinLateral("split(id) as (word, length)")
                .select("id, ts, word, length");
        tableEnv.toDataStream(resultTable).print();

        // 4.2 SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("SELECT id, ts, word, length " +
                "FROM sensor, lateral table(split(id)) as split(word, length)");
        tableEnv.toDataStream(resultSqlTable).print();

        execute();
    }

    public static class Split extends TableFunction<Tuple2<String, Integer>> {

        private static final long serialVersionUID = -3769199678887141414L;

        // 定义属性，分隔符
        private String separator = ",";

        public Split(String separator) {
            this.separator = separator;
        }

        // 必须实现一个eval方法，没有返回值
        public void eval(String str) {
            for (String s : str.split(this.separator)) {
                this.collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
