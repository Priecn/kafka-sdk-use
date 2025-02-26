package org.example.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithGraceFullShutdown {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithGraceFullShutdown.class.getSimpleName());
    private static final String GROUP_ID = "my-java-application";
    private static final String TOPIC = "demo_java";

    public static void main(String[] args) {

        LOGGER.info("Consumes String message to demo_java topic");

        // create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        /* to deserialize key and value from bytes after reading from the kafka broker
         * Here the consumer will be expecting key and value of type string
         */
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", GROUP_ID);

        /*
            none -> If broker does not have consumer group with it will fail, so we need to create consumer group before reading message
            earliest -> reads from start of topic [--from-beginning]
            latest -> reads new message
         */
        properties.setProperty("auto.offset.reset", "earliest");

        // create the Consumer
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {

            // get current main thread
            final Thread mainThread = Thread.currentThread();

            // adding shutDown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");

                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            // subscribe to topic
            kafkaConsumer.subscribe(List.of(TOPIC));
            LOGGER.info("Polling for message...");
            // poll for data
            while (true) {

                ConsumerRecords<String, String> consumedRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                consumedRecords.forEach(consumedRecord -> LOGGER.info("Key: {} \t Value: {} \t Partition: {} \t Offset: {} \n",
                        consumedRecord.key(), consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset()));
            }

        } catch (WakeupException e) {
            LOGGER.info("Consumer is going to shutdown.");
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage(), exception);
        } finally {
            LOGGER.info("The consumer is now gracefully shutdown.");
        }
    }
}