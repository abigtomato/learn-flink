package org.abigtomato.learn.udf;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义标量函数
 *
 * @author abigtomato
 */
public class ScalarFunctionExample extends BaseContext {

    public static void main(String[] args) throws Exception {
        DataStream<SensorReading> dataStream = initTextData(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 自定义标量函数，实现求id的hash值
        // table API
        tableEnv.createTemporarySystemFunction("hashCode", new HashCode(13));
        Table resultTable = sensorTable.select("id, ts, hashCode(id)");
        tableEnv.toDataStream(resultTable).print();

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("SELECT id, ts, hashCode(id) FROM sensor");
        tableEnv.toDataStream(resultSqlTable).print();

        execute();
    }

    public static class HashCode extends ScalarFunction {

        private static final long serialVersionUID = -165904959734093492L;

        private int factor = 13;

        public HashCode(int factor) {
            this.factor = factor;
        }

        public int eval(String str) {
            return str.hashCode() * this.factor;
        }
    }
}
