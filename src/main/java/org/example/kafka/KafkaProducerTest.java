package org.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author machenggong
 * @since 2021/4/19
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Object, String> producer = new KafkaProducer<Object, String>(props);
        int totalMessageCount = 10000;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            System.out.println("发送数据-->"+value);
            producer.send(new ProducerRecord<Object, String>("flink-kafka-topic", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(1000);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }

}
