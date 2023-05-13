package org.abigtomato.learn.udf;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * 自定义聚合函数
 *
 * @author abigtomato
 */
public class AggregateFunctionExample extends BaseContext {

    public static void main(String[] args) throws Exception {
        DataStream<SensorReading> dataStream = initTextData(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        Table sensorTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp");

        // 自定义聚合函数，求当前传感器的平均温度值
        // table API
        tableEnv.createTemporarySystemFunction("avgTemp", new AvgTemp());
        Table resultTable = sensorTable.groupBy("id")
                .aggregate("avgTemp(temp) as avgTemp")
                .select("id, avgTemp");
        tableEnv.toDataStream(resultTable).print();

        // SQL
        tableEnv.createTemporaryView("sensor", sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("SELECT id, avgTemp(temp) " +
                " FROM sensor GROUP BY id");
        tableEnv.toDataStream(resultSqlTable).print();

        execute();
    }

    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        private static final long serialVersionUID = -3082643461437722358L;

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        // 必须实现一个accumulate方法，来数据之后更新状态
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }
    }
}
