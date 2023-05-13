package org.abigtomato.learn.transform;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 基础转换算子 map、flatMap、filter
 *
 * @author abigtomato
 */
@Slf4j
public class Base {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.readTextFile("D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\sensor.csv");

        SingleOutputStreamOperator<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {

            private static final long serialVersionUID = -6303640366951057494L;

            @Override
            public Integer map(String s) {
                return s.length();
            }
        });
        mapStream.print("map");

        SingleOutputStreamOperator<String> flatMapStream = inputStream.flatMap(new FlatMapIterator<String, String>() {

            private static final long serialVersionUID = -1664668143337044365L;

            @Override
            public Iterator<String> flatMap(String s) {
                String[] fields = s.split(",");
                return Lists.newArrayList(fields).iterator();
            }
        });
        flatMapStream.print("flatMap");

        SingleOutputStreamOperator<String> flatMapStream2 = inputStream.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = -8837871750007689761L;

            @Override
            public void flatMap(String s, Collector<String> collector) {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });
        flatMapStream2.print("flatMap2");

        SingleOutputStreamOperator<String> filterStream = inputStream.filter(new FilterFunction<String>() {

            private static final long serialVersionUID = -5901214520251220724L;

            @Override
            public boolean filter(String s) {
                return s.startsWith("sensor_1");
            }
        });
        filterStream.print("filter");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
            log.debug("error", e.getCause());
        }
    }
}
