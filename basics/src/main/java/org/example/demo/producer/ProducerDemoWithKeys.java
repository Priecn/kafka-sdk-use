package org.example.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {

        LOGGER.info("Producing String message to demo_java topic and listen to callback");

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
            LOGGER.info("As in both of the stream below we are using same key list, same keys should go to same partition");
            // create a producer Record
            IntStream.range(0,10)
                    .mapToObj(i -> new ProducerRecord<>("demo_java", "id_ " + i, "Message " + i))
                    .forEach(producerRecord -> {
                        // send data
                        kafkaProducer.send(producerRecord, (recordMetadata,e) -> {
                            // executed every time a record is successfully sent or an exception is thrown
                            if (e == null) {
                                LOGGER.info("Received new metadata: \n key: {} \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                                        producerRecord.key(), recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            } else {
                                LOGGER.error("Error occurred while Producing message: {}", e.getMessage(), e);
                            }
                        });
                    });

            IntStream.range(0,10)
                    .mapToObj(i -> new ProducerRecord<>("demo_java", "id_ " + i, "New Message " + i))
                    .forEach(producerRecord -> {
                        // send data
                        kafkaProducer.send(producerRecord, (recordMetadata,e) -> {
                            // executed every time a record is successfully sent or an exception is thrown
                            if (e == null) {
                                LOGGER.info("Received new metadata: \n key: {} \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                                        producerRecord.key(), recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            } else {
                                LOGGER.error("Error occurred while Producing message: {}", e.getMessage(), e);
                            }
                        });
                        });


            // flush and close the Producer
            kafkaProducer.flush();

        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            throw exception;
        }
    }
}