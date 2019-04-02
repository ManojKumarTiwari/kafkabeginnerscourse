package com.maddy.kafkalearn.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++){
            // create a producer record

            String topic = "first_topic";
            String value = "Hello from Java producer" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

            // send data - asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        // an exception was thrown
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // flush data
        kafkaProducer.flush();

        // or flush data and close producer
        kafkaProducer.close();

    }
}
