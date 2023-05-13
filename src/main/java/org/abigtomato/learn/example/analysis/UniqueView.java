package org.abigtomato.learn.example.analysis;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.abigtomato.learn.FlinkContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.nio.charset.Charset;
import java.util.HashSet;

/**
 * @author abigtomato
 */
public class UniqueView {

    public static void main(String[] args) {
        FlinkContext context = FlinkContext.init(1);

        String path = "D:\\WorkSpace\\javaproject\\Nebula\\nebula-base-flink\\src\\main\\resources\\UserBehavior.csv";
        DataStream<String> inputDataStream = context.getStreamDataFromText(path);

        DataStream<UserBehavior> userBehaviorDataStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]),
                    new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>)
                        (userBehavior, recordTimestamp) -> userBehavior.getTimestamp() * 1000L));

        AllWindowedStream<UserBehavior, TimeWindow> allWindowedStream = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)));

        // 使用HashSet去重
        allWindowedStream.apply(new AllWindowFunction<UserBehavior, PageViewCount, TimeWindow>() {

            private static final long serialVersionUID = -6660843201520887240L;

            @Override
            public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<PageViewCount> out) {
                HashSet<Long> uidSet = new HashSet<>();
                for (UserBehavior ub : values) {
                    uidSet.add(ub.getUserId());
                }
                out.collect(new PageViewCount("uv", window.getEnd(), (long) uidSet.size()));
            }
        }).print("HashSetUV");

        // 使用布隆过滤器去重
        allWindowedStream.trigger(new Trigger<UserBehavior, TimeWindow>() {

            private static final long serialVersionUID = 3231617537754407073L;

            @Override
            public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) {
                return TriggerResult.FIRE_AND_PURGE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) {
            }
        }).process(new ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>() {

            private static final long serialVersionUID = 3782046160566199645L;

            private BloomFilter<CharSequence> bloomFilter;
            private Jedis jedis;

            @Override
            public void open(Configuration parameters) {
                this.bloomFilter = BloomFilter.create(Funnels
                        .stringFunnel(Charset.defaultCharset()), 100000000, 0.0001);
                this.jedis = new Jedis("192.168.1.105", 6379);
            }

            @Override
            public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) {
                String hashKey = "uv_count";
                String hashField = String.valueOf(context.window().getEnd());

                UserBehavior userBehavior = elements.iterator().next();
                String userId = String.valueOf(userBehavior.getUserId());
                if (!this.bloomFilter.mightContain(userId)) {
                    this.bloomFilter.put(userId);

                    Long uvCount = 0L;
                    String uvCountStr = this.jedis.hget(hashKey, hashField);
                    if (StringUtils.isNotBlank(uvCountStr)) {
                        uvCount = Long.parseLong(uvCountStr) + 1;
                    }

                    this.jedis.hset(hashKey, hashField, String.valueOf(uvCount));
                }
            }
        }).print("BloomFilterUV");

        context.execute();
    }
}
