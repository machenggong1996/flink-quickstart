package org.example.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author machenggong
 * @since 2021/4/15
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 8
        env.setParallelism(8);
//        String inputPath = "/Users/machenggong/project/flink-quickstart/src/main/resources/hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // nc -lk 9000
        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");
        DataStream<String> inputDataStream = env.socketTextStream(host, port);


        SingleOutputStreamOperator<Tuple2<String, Integer>> map = inputDataStream.flatMap(new WordCount.MyFlatMapper());
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);
        sum.print();
        env.execute();
    }

}
