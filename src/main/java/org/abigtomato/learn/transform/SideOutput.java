package org.abigtomato.learn.transform;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 旁路输出 OutputTag、process、CoMap、Connect、union
 *
 * @author abigtomato
 */
@Slf4j
public class SideOutput extends BaseContext {

    static final OutputTag<SensorReading> STRING_OUTPUT_TAG = new OutputTag<>("side-output", TypeInformation.of(SensorReading.class));

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initTextData(1);

        // 侧输出流
        SingleOutputStreamOperator<SensorReading> processStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {

            private static final long serialVersionUID = 3206034611310424781L;

            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) {
                // 温度大于30的走测流，否则走主流
                if (sensorReading.getTemperature() > 30) {
                    context.output(STRING_OUTPUT_TAG, sensorReading);
                } else {
                    collector.collect(sensorReading);
                }
            }
        });
        processStream.print("主流输出");

        // 获取支流
        DataStream<SensorReading> sideOutputStream = processStream.getSideOutput(STRING_OUTPUT_TAG);
        sideOutputStream.print("侧流输出");

        // 合并流
        ConnectedStreams<SensorReading, SensorReading> connectedStreams = processStream.connect(sideOutputStream);

        // 多流操作算子
        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<SensorReading, SensorReading, Object>() {

            private static final long serialVersionUID = -6882054224157984733L;

            @Override
            public Object map1(SensorReading sensorReading) {
                return new Tuple2<>(sensorReading.getId(), "normal");
            }

            @Override
            public Object map2(SensorReading sensorReading) {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(), "high temp warning");
            }
        });
        resultStream.print("合并流输出");

        // 联合多流
        DataStream<SensorReading> unionStream = processStream.union(sideOutputStream);
        unionStream.print("unionStream");

        execute();
    }
}
