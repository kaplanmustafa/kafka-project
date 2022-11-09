package org.kafka.sample.streams;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import jdk.jfr.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("start kafka producer!!");
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "wikimedia";

        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Set some producer parameters related with performance
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // create an eventhandler which will aallow us to handle events and send them to kafka
        EventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        String url = "http://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(2);
    }
}
