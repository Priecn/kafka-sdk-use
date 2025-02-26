package org.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        LOGGER.info("Producing String message to demo_java topic");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        /* to serialize key and value to bytes before sending to the kafka broker
         * Here the producer will be expecting key and value of type string
         */
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {

            // create a producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

            // send data
            kafkaProducer.send(producerRecord);

            // flush and close the Producer
            kafkaProducer.flush();

        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            throw exception;
        }
    }
}