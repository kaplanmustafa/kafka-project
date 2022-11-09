package org.kafka.sample;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class ConsumerDemoCooperative {

    private static  final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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
        prop.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, UUID.randomUUID().toString());

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // get a reference to main thread
        final Thread mainThread = Thread.currentThread();


        // Add a shutdown hook and wakeup the consumer which will throw an exception
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info(" shutdown method is called ");
                consumer.wakeup();

                try{
                    mainThread.join();
                } catch (InterruptedException e ){
                    e.printStackTrace();
                }
            }
        });

        try {
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
        } catch (WakeupException e) {
            log.info("Wakeup exception occurred");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Consumer is gracefully shutdown !!");
        }

    }
}
