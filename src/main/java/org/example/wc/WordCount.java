package org.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 数据集
 * @author machenggong
 * @since 2021/4/15
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "/Users/machenggong/project/flink-quickstart/src/main/resources/hello.txt";
        // DataSet
        DataSource<String> inputDataSet = env.readTextFile(inputPath);
        // 对数据集进行处理 按照第一个位置的word进行分组 将第二个位置上的数据进行求和
        DataSet<Tuple2<String, Integer>> sum = inputDataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);
        sum.print();
    }

    //自定义类 实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
