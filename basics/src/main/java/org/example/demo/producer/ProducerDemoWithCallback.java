package org.example.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

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

            // create a producer Record
//            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
            IntStream.range(0,10)
                    .mapToObj(i -> new ProducerRecord<>("demo_java", "Key " + i, "Message " + i))
                    .forEach(producerRecord -> {
                        // send data
                        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                            // executed every time a record is successfully sent or an exception is thrown
                            if (e == null) {
                                LOGGER.info("Received new metadata: \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                            } else {
                                LOGGER.error("Error occurred while Producing message: {}", e.getMessage(), e);
                            }
                        });
                    });

            LOGGER.info("Without key kafka will optimize and send message in batch so will go to same partition [StickyPartitioner]");
            // StickyPartitioner Default [batch.size = 16384] 16KB
            // can be updated using
//            properties.setProperty("batch.size", "50"); // never go for smaller batch size
            // Default partitioner.class=null
            // can be updated using (not recommended)
//            properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); // this will send each batch to different partition
            for (int i=0; i<30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
                kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                    // executed every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        LOGGER.info("Received new metadata: \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        LOGGER.error("Error occurred while Producing message: {}", e.getMessage(), e);
                    }
                });
            }

            // flush and close the Producer
            kafkaProducer.flush();

        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            throw exception;
        }
    }
}