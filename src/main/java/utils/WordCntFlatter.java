package utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 自定义类，实现FlatMapFunction接口
public class WordCntFlatter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
        // 按空格分词
        String[] words = s.split(" ");
        // 遍历所有word，包成二元组输出
        for (String str : words) {
            out.collect(new Tuple2<>(str, 1));
        }
    }
}
