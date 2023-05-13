package org.abigtomato.learn.example.windcontrol;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author abigtomato
 */
public class TxPayMatch {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init();

        String orderLogPath = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\OrderLog.csv";
        DataStream<OrderEvent> orderEventStream = context.getStreamDataFromText(orderLogPath).map(line -> {
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
                })).filter(data -> !"".equals(data.getTxId()));

        String receiptLogPath = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\ReceiptLog.csv";
        DataStream<ReceiptEvent> receiptEventStream = context.getStreamDataFromText(receiptLogPath).map(line -> {
            String[] fields = line.split(",");
            return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<ReceiptEvent>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<ReceiptEvent>() {

                    private static final long serialVersionUID = 2698616680685802307L;

                    @Override
                    public long extractTimestamp(ReceiptEvent element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        KeyedStream<OrderEvent, String> orderEventKeyedStream = orderEventStream.keyBy(OrderEvent::getTxId);
        KeyedStream<ReceiptEvent, String> receiptEventKeyedStream = receiptEventStream.keyBy(ReceiptEvent::getTxId);

        DataStream<Tuple2<OrderEvent, ReceiptEvent>> joinResultStream = orderEventKeyedStream
                .intervalJoin(receiptEventKeyedStream)
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>() {

                    private static final long serialVersionUID = 2042797195335551607L;

                    @Override
                    public void processElement(OrderEvent left, ReceiptEvent right, Context ctx,
                                               Collector<Tuple2<OrderEvent, ReceiptEvent>> out) {
                        out.collect(new Tuple2<>(left, right));
                    }
                });
        joinResultStream.print("joinResult");

        OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays") {
            private static final long serialVersionUID = 2561790410527370365L;
        };
        OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts") {
            private static final long serialVersionUID = 7570642389732808088L;
        };

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> connectResultStream = orderEventKeyedStream
                .connect(receiptEventKeyedStream)
                .process(new CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>() {

                    private static final long serialVersionUID = -8450674142022141040L;

                    private ValueState<OrderEvent> payState;
                    private ValueState<ReceiptEvent> receiptState;

                    @Override
                    public void open(Configuration parameters) {
                        this.payState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("pay", OrderEvent.class));
                        this.receiptState = this.getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("receipt", ReceiptEvent.class));
                    }

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context ctx,
                                                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                        ReceiptEvent receiptEvent = this.receiptState.value();
                        if (receiptEvent != null) {
                            out.collect(new Tuple2<>(orderEvent, receiptEvent));
                            this.payState.clear();
                            this.receiptState.clear();
                        } else {
                            ctx.timerService().registerEventTimeTimer((orderEvent.getTimestamp() + 5) * 1000L);
                            this.payState.update(orderEvent);
                        }
                    }

                    @Override
                    public void processElement2(ReceiptEvent receiptEvent, Context ctx,
                                                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                        OrderEvent orderEvent = this.payState.value();
                        if (orderEvent != null) {
                            out.collect(new Tuple2<>(orderEvent, receiptEvent));
                            this.payState.clear();
                            this.receiptState.clear();
                        } else {
                            ctx.timerService().registerEventTimeTimer((receiptEvent.getTimestamp() + 3) * 1000L);
                            this.receiptState.update(receiptEvent);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                        if (this.payState.value() != null) {
                            ctx.output(unmatchedPays, this.payState.value());
                        }
                        if (this.receiptState.value() != null) {
                            ctx.output(unmatchedReceipts, this.receiptState.value());
                        }
                        this.payState.clear();
                        this.receiptState.clear();
                    }
                });
        connectResultStream.print("connectResult");
        connectResultStream.getSideOutput(unmatchedPays).print("unmatchedPays");
        connectResultStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts");

        context.execute();
    }
}
