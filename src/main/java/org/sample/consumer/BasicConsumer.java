package org.sample.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BasicConsumer {
    private static Logger log = LoggerFactory.getLogger(BasicConsumer.class.getName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String topic = "topic_1";
        String groupId = "my-basic-consumer";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);

        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        Thread main = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, trying to close consumer");
            consumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            while (true) {
                log.info("polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    log.info(
                            "value: " + record.value() + " | " +
                                    "offset: " + record.offset() + " | " +
                                    "timestamp: " + record.timestamp() + " | " +
                                    "partition: " + record.partition()
                    );
                });
            }
        } catch (WakeupException we) {
            log.info("WakeupException detected");
        } finally {
            consumer.close();
            log.info("graceful shutdown of " + groupId);
        }

    }
}
