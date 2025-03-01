package org.example.wikimedia.producer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class WikimediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "wikimedia_recent_changes";
    private static final String STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) {
        LOGGER.info("Producing wikimedia changes");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // set high throughput producer config
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20 ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB

        // create the Producer
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            try (EventSource eventSource = new EventSource.Builder(URI.create(STREAM_URL)).build()) {
                Timer timer = new Timer();

                TimerTask action = new TimerTask() {
                    public void run() {
                        eventSource.close();//close the event source
                        kafkaProducer.flush(); // close kafka producer
                        timer.cancel();
                        LOGGER.info("Closing producer!");
                    }
                };
                // We produce for X seconds and close event source after that
                timer.schedule(action, 10000);
                Iterable<MessageEvent> messages = eventSource.messages();
                while (messages.iterator().hasNext()) {
                    MessageEvent messageEvent = messages.iterator().next();
                    String data = messageEvent.getData();
                    LOGGER.info("Received data on stream: {}", data);
                    kafkaProducer.send(new ProducerRecord<>(TOPIC, data));
                }
            }

        } catch (Exception exception) {
            LOGGER.error(exception.getMessage(), exception);
        }
    }
}