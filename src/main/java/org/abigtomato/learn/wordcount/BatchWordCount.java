package org.abigtomato.learn.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 批处理WordCount
 */
public class BatchWordCount {

    public static void main(String[] args) {
        // 运行时上下文环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        DataSource<String> lineDS = env.readTextFile("input/words.txt");

        // 对一行文字进行分词转换
        // returns 方法指定的返回数据类型 Tuple2
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (line, out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 使用索引定位分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        // 使用索引定位聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        try {
            sum.print();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
