package org.abigtomato.learn.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 自定义并行数据源
 */
public class ParallelSourceExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new ParallelSourceFunction<Integer>() {

            private boolean running = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) {
                while (running) {
                    sourceContext.collect(random.nextInt());
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(2).print();
    }
}
