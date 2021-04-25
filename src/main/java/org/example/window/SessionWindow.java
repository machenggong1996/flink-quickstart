package org.example.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @author machenggong
 * @since 2021/4/25
 */
public class SessionWindow {

    public static void main(String[] args) {

        // 设置执行环境, 类似spark中初始化sparkContext

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                long timeMillis = System.currentTimeMillis();

                int random = new Random().nextInt(10);

                System.err.println("value : " + value + " random : " + random + " timestamp : " + timeMillis + "|" + format.format(timeMillis));

                return new Tuple2<String, Integer>(value, random);
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);

        //如果连续10s内，没有数据进来，则会话窗口断开。
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        // window.sum(1).print();

        window.apply(new TumblingWindow.MyTimeWindowFunction()).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
