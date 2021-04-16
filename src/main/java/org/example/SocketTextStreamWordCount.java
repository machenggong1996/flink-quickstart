package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author machenggong
 * @since 2021/4/15
 */
public class SocketTextStreamWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostName = args[0];
        int port = Integer.parseInt(args[1]);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = environment.socketTextStream(hostName, port);
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter()).keyBy(0).sum(1);
        counts.print();
        environment.execute("Java WordCount from SocketTextStream Example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

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

}
