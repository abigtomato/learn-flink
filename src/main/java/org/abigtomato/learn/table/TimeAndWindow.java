package org.abigtomato.learn.table;

import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @author abigtomato
 */
public class TimeAndWindow extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env());

        DataStream<SensorReading> timeDataStream = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {

                    private static final long serialVersionUID = -8971339563488477201L;

                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        Table dataTable = tableEnv.fromDataStream(timeDataStream,
                $("id"), $("timestamp").rowtime(), $("temperature"));

        // Group Window
        // table API
        Table groupTable = dataTable
                .window(Tumble.over(lit(10).seconds()).on($("timestamp")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count(), $("temperature").avg(), $("tw").end());
        tableEnv.toDataStream(groupTable).print();

        // SQL
//        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
//                "from sensor group by id, tumble(rt, interval '10' second)");
//
        // Over Window
        // table API
//        Table overResult = dataTable
//                .window(Over.partitionBy($("id")).orderBy($("timestamp"))
//                        .preceding(rowInterval(2L)).as("ow"))
//                .select($("id"), $("timestamp"), $("id").count().over("ow"),
//                        $("temperature").avg().over("ow"));
//        tableEnv.toDataStream(overResult).print();

        // SQL
//        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow " +
//                " from sensor " +
//                " window ow as (partition by id order by rt rows between 2 preceding and current row)");

        execute();
    }
}
