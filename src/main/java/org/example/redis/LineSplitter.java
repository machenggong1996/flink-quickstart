package org.example.redis;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author machenggong
 * @since 2021/4/23
 */
public class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] tokens = s.toLowerCase().split("\\W+");
        for (String token : tokens) {
            if (token.length() > 0) {
                collector.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }

}
