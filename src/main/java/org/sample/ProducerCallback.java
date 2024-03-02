package org.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerCallback {
    private static Logger log = LoggerFactory.getLogger(ProducerCallback.class.getName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        String topic = "topic_1";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        properties.put("batch.size", 2);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        log.info("sending record...");
        sendData(producer, topic);

        producer.flush();
        producer.close();

        log.info("closed");
    }

    private static void sendData(KafkaProducer<String, String> producer, String topic) {
        IntStream.range(1, 10)
                .forEach(i -> {
                    producer.send(new ProducerRecord<>(topic, "data_" + i), (recordMetadata, e) -> log.info(
                            "metadata received \n" +
                                    "topic: " + recordMetadata.topic() + "\n" +
                                    "offset: " + recordMetadata.offset() + "\n" +
                                    "partition: " + recordMetadata.partition() + "\n" +
                                    "timestamp: " + recordMetadata.timestamp() + "\n"
                    ));
                });
    }
}
