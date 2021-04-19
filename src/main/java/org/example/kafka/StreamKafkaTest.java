package org.example.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author machenggong
 * @since 2021/4/19
 */
public class StreamKafkaTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-kafka");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest"); //value 反序列化

        FlinkKafkaConsumer<String> consumer =new FlinkKafkaConsumer<String>(
                "flink-kafka-topic",  //kafka topic 这里改成需要的kafka主题
                new SimpleStringSchema(),  // String 序列化
                props);
        //设置水位线
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        DataStream<Tuple2<String, Long>> keyedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                //10秒统计数据并做均值计算
                .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                        long sum = 0L;
                        int count = 0;
                        for (Tuple2<String, Long> record: input) {
                            sum += record.f1;
                            count++;
                        }
                        Tuple2<String, Long> result = input.iterator().next();
                        result.f1 = sum / count;
                        out.collect(result);
                    }
                });
        //key流写入文件 参数一 args[0]
        keyedStream.print();
        env.execute("Flink-Kafka sample");
    }

    public static class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            if (value != null && value.contains(",")) {
                String[] parts = value.split(",");
                out.collect(new Tuple2<String, Long>(parts[1], Long.parseLong(parts[2])));
            }
        }
    }

    public static class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            if (lastElement != null && lastElement.contains(",")) {
                String[] parts = lastElement.split(",");
                return new Watermark(Long.parseLong(parts[0]));
            }
            return null;
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            if (element != null && element.contains(",")) {
                String[] parts = element.split(",");
                return Long.parseLong(parts[0]);
            }
            return 0L;
        }
    }

}
