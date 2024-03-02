package org.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class BasicProducer {
    private static Logger log = LoggerFactory.getLogger(BasicProducer.class.getName());

    public static void main(String[] args) {
        log.info("Kafka Producer");

        String topic = "topic_1";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        log.info("sending record...");
        IntStream.range(1, 10)
                .forEach(i -> producer.send(new ProducerRecord<>(topic, "data_" + i)));

        producer.flush();
        producer.close();

        log.info("closed");
    }
}
