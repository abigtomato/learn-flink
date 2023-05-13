package org.abigtomato.learn.example.windcontrol;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author abigtomato
 */
public class OrderTimeout {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init();

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\OrderLog.csv";
        KeyedStream<OrderEvent, Long> orderEventKeyedStream = context.getStreamDataFromText(path).map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {

                    private static final long serialVersionUID = 2698616680685802307L;

                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                })).keyBy(OrderEvent::getOrderId);

        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {

                    private static final long serialVersionUID = 4979637953268733012L;

                    @Override
                    public boolean filter(OrderEvent value) {
                        return "create".equals(value.getEventType());
                    }
                }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {

                    private static final long serialVersionUID = -1311584630797809446L;

                    @Override
                    public boolean filter(OrderEvent value) {
                        return "pay".equals(value.getEventType());
                    }
                }).within(Time.minutes(15));

        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout") {
            private static final long serialVersionUID = 6085879843297254944L;
        };

        PatternStream<OrderEvent> orderEventPatternStream = CEP.pattern(orderEventKeyedStream, orderEventPattern);

        orderEventPatternStream.select(orderTimeoutTag, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            private static final long serialVersionUID = 7691545882827125124L;

            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) {
                Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
                return new OrderResult(timeoutOrderId, "timeout " + timeoutTimestamp);
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {

            private static final long serialVersionUID = 5093683388621868078L;

            @Override
            public OrderResult select(Map<String, List<OrderEvent>> pattern) {
                Long payedOrderId = pattern.get("pay").iterator().next().getOrderId();
                return new OrderResult(payedOrderId, "payed");
            }
        });

        context.execute();
    }
}
