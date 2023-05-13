package org.abigtomato.learn.connector;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.abigtomato.learn.FlinkContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;
import java.util.Random;

/**
 * 自定义数据源
 *
 * @author abigtomato
 */
@Slf4j
public class SourceUdf {

    public static void main(String[] args) throws Exception {
        FlinkContext context = FlinkContext.init(1);
        StreamExecutionEnvironment env = context.env();

        env.addSource(new SourceFunction<Object>() {

            private static final long serialVersionUID = -1136997809296761485L;

            private boolean running = true;

            @Override
            public void run(SourceContext<Object> sourceContext) throws Exception {
                Random random = new Random();
                Map<String, Double> sensorTempMap = Maps.newHashMap();
                for (int i = 0; i < 10; i++) {
                    sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
                }
                while (this.running) {
                    for (String sensorId : sensorTempMap.keySet()) {
                        double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                        sensorTempMap.put(sensorId, newTemp);
                        sourceContext.collect(sensorId + System.currentTimeMillis() + newTemp);
                    }
                    Thread.sleep(1000L);
                }
            }

            @Override
            public void cancel() {
                this.running = false;
            }
        }).print();

        context.execute();
    }
}
