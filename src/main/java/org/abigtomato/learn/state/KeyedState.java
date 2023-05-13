package org.abigtomato.learn.state;

import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.BaseContext;
import org.abigtomato.learn.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

/**
 * 键控状态
 *
 * @author abigtomato
 */
@Slf4j
public class KeyedState extends BaseContext {

    public static void main(String[] args) {
        DataStream<SensorReading> dataStream = initSocketData(1);

        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(SensorReading::getId)
                .map(new RichMapFunction<SensorReading, Integer>() {

                    private static final long serialVersionUID = -817319575277264148L;

                    private ValueState<Integer> keyCountState;
                    private ListState<String> keyListState;
                    private MapState<String, Double> keyMapState;
                    private ReducingState<SensorReading> keyReducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.keyCountState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("key-count", Integer.class));
                        this.keyListState = this.getRuntimeContext()
                                .getListState(new ListStateDescriptor<>("key-list", String.class));
                        this.keyMapState = this.getRuntimeContext()
                                .getMapState(new MapStateDescriptor<>("key-map", String.class, Double.class));
                        this.keyReducingState = this.getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<>("key-reduce",
                                        (ReduceFunction<SensorReading>) (value1, value2) -> new SensorReading(), SensorReading.class));
                    }

                    @Override
                    public Integer map(SensorReading sensorReading) throws Exception {
                        this.keyListState.add("sayHello");
                        for (String state : this.keyListState.get()) {
                            log.info("-----> state: {}", state);
                        }

                        this.keyMapState.put("sayHello", 2.33);
                        for (Map.Entry<String, Double> entry : this.keyMapState.entries()) {
                            log.info("-----> entry: {}", entry);
                        }

                        this.keyReducingState.add(new SensorReading());
                        log.info("-----> sensorReading: {}", this.keyReducingState.get());

                        Integer count = this.keyCountState.value();
                        count++;
                        this.keyCountState.update(count);
                        return count;
                    }
                });
        resultStream.print();

        execute();
    }
}
