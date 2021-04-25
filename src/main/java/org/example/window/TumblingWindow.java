package org.example.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * @author machenggong
 * @since 2021/4/25
 */
public class TumblingWindow {

    public static void main(String[] args) {
        //设置执行环境，类似spark中初始化sparkContext
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9000);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {

                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                long timeMillis = System.currentTimeMillis();

                int random = new Random().nextInt(10);

                System.out.println("value: " + value + " random: " + random + "timestamp: " + timeMillis + "|" + format.format(timeMillis));

                return new Tuple2<String, Integer>(value, random);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);


        // 基于时间驱动，每隔10s划分一个窗口
        //WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(10));

        // 基于事件驱动, 每相隔3个事件(即三个相同key的数据), 划分一个窗口进行计算
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(3);

        // apply是窗口的应用函数，即apply里的函数将应用在此窗口的数据上。
        //timeWindow.apply(new MyTimeWindowFunction()).print();
        countWindow.apply(new MyCountWindowFunction()).print();

        try {
            // 转换算子都是lazy init的, 最后要显式调用 执行程序
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 基于时间
     */
    public static class MyTimeWindowFunction implements WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            int sum = 0;

            for (Tuple2<String, Integer> tuple2 : input) {
                sum += tuple2.f1;
            }

            long start = window.getStart();
            long end = window.getEnd();

            out.collect("key:" + tuple.getField(0) + " value: " + sum + "| window_start :"
                    + format.format(start) + "  window_end :" + format.format(end)
            );
        }
    }

    /**
     * 基于事件
     */
    public static class MyCountWindowFunction implements WindowFunction<Tuple2<String, Integer>, String, Tuple, GlobalWindow> {

        @Override
        public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            int sum = 0;

            for (Tuple2<String, Integer> tuple2 : input) {
                sum += tuple2.f1;
            }
            //无用的时间戳，默认值为： Long.MAX_VALUE,因为基于事件计数的情况下，不关心时间。
            long maxTimestamp = window.maxTimestamp();

            out.collect("key:" + tuple.getField(0) + " value: " + sum + "| maxTimeStamp :"
                    + maxTimestamp + "," + format.format(maxTimestamp)
            );
        }
    }

}
