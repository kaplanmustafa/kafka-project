package org.kafka.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static  final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Start Kafka consumer");
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "havadurumu";
        String groupid = "java-consumer-group";

        // create kafka consumer properties
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // subscribe to a topic or topics
        consumer.subscribe(Collections.singleton(topic));

        // fetch messages until cancel   ---  calling .poll() method
        while (true) {
            log.info("Start polling....");
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(3000));
            //iterate over records and process !!
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key : " + record.key() + ", Value : " + record.value());
                log.info("Partition : " + record.partition() + ", Offset : " + record.offset());
            }
        }

    }
}
