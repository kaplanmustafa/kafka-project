package org.kafka.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());

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
        for (int i=0; i<10; i++){
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, "istanbulda sıcaklık " + i + " derece");

            // Send data   -- asyncronous
            producer.send(producerRecord, new Callback() {
                // execute when a message is successfully received
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        log.info ("Received message is :" + "\n" +
                                  "Topic : " + metadata.topic() + "\n" +
                                  "Partition : " + metadata.partition() + "\n" +
                                  "Offset : " + metadata.offset() + "\n" +
                                  "Timestamp : " + metadata.timestamp() + "\n" +
                                  "Key : " + producerRecord.value() + "\n" +
                                  "Value : " + producerRecord.value()
                                );
                    } else {
                        log.error("An error occurred", e);
                    }
                }
            });
            try {
                Thread.sleep(3000);
            } catch( InterruptedException e) {
                e.printStackTrace();
            }
            producer.flush();
        }

        log.info("Send the message from producer");


        // Flush and close the producer  --syncrounous

        // Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
        // greater than 0) and blocks on the completion of the requests associated with these records.
        // A request is considered completed when it is successfully acknowledged

        // This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
        // If the producer is unable to complete all requests before the timeout expires, this method will fail
        // any unsent and unacknowledged records immediately.
        producer.close();
    }
}
