package org.kafka.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("start kafka producer!!");
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "havadurumu";
        // Create producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // Create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, "istanbulda sıcaklık 30 derece");

        // Send data   -- asyncronous
        producer.send(producerRecord);
        log.info("Send the message from producer");

        // Flush and close the producer  --synrounous

        // Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
        // greater than 0) and blocks on the completion of the requests associated with these records.
        // A request is considered completed when it is successfully acknowledged
        producer.flush();

        // This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
        // If the producer is unable to complete all requests before the timeout expires, this method will fail
        // any unsent and unacknowledged records immediately.
        producer.close();
    }
}
