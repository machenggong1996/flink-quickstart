package org.example.split;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author machenggong
 * @since 2021/4/21
 */
public class SplitStreamBySplit {

    public static void main(String[] args) throws Exception{

        /**运行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**输入数据源*/
        DataStreamSource<Tuple3<String, String, String>> source = env.fromElements(
                new Tuple3<>("productID1", "click", "user_1"),
                new Tuple3<>("productID1", "click", "user_2"),
                new Tuple3<>("productID1", "browse", "user_1"),
                new Tuple3<>("productID2", "browse", "user_1"),
                new Tuple3<>("productID2", "click", "user_2"),
                new Tuple3<>("productID2", "click", "user_1")
        );

        /**1、定义拆分逻辑*/
        SplitStream<Tuple3<String, String, String>> splitStream = source.split(new OutputSelector<Tuple3<String, String, String>>() {
            @Override
            public Iterable<String> select(Tuple3<String, String, String> value) {

                ArrayList<String> output = new ArrayList<>();
                if (value.f0.equals("productID1")) {
                    output.add("productID1");

                } else if (value.f0.equals("productID2")) {
                    output.add("productID2");
                }

                return output;

            }
        });

        /**2、将流真正拆分出来*/
        splitStream.select("productID1").print();

        env.execute();
    }

}
