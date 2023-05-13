package org.abigtomato.learn.example.windcontrol;

import org.abigtomato.learn.FlinkContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author abigtomato
 */
public class LoginFail {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init();

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\LoginLog.csv";
        KeyedStream<LoginEvent, Long> loginEventKeyedStream = context.getStreamDataFromText(path).map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {

                    private static final long serialVersionUID = 5463267052473158072L;

                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long recordTimestamp) {
                        return loginEvent.getTimestamp() * 1000L;
                    }
                })).keyBy(LoginEvent::getUserId);

        Pattern.<LoginEvent>begin("firstFail").where(new SimpleCondition<LoginEvent>() {

            private static final long serialVersionUID = 8818190580718255403L;

            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getLoginState());
            }
        }).next("secondFail").where(new SimpleCondition<LoginEvent>() {

            private static final long serialVersionUID = -5585899926319137531L;

            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getLoginState());
            }
        }).next("").where(new SimpleCondition<LoginEvent>() {

            private static final long serialVersionUID = -673628716545576850L;

            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getLoginState());
            }
        });

        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern
                .<LoginEvent>begin("failEvents")
                .where(new SimpleCondition<LoginEvent>() {

                    private static final long serialVersionUID = -1201101152088808856L;

                    @Override
                    public boolean filter(LoginEvent value) {
                        return "fail".equals(value.getLoginState());
                    }
                }).times(3).consecutive().within(Time.seconds(5));

        PatternStream<LoginEvent> loginEventPatternStream = CEP.pattern(loginEventKeyedStream, loginFailPattern);
        DataStream<LoginFailWarning> warningStream = loginEventPatternStream
                .select(new PatternSelectFunction<LoginEvent, LoginFailWarning>() {

                    private static final long serialVersionUID = -7534865070691808270L;

                    @Override
                    public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) {
                        List<LoginEvent> failEvents = pattern.get("failEvents");
                        LoginEvent firstFailEvent = failEvents.get(0);
                        LoginEvent lastFailEvent = failEvents.get(failEvents.size() - 1);
                        return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimestamp(),
                                lastFailEvent.getTimestamp(), "login fail " + failEvents.size() + " times");
                    }
                });
        warningStream.print();

        context.execute();
    }
}
