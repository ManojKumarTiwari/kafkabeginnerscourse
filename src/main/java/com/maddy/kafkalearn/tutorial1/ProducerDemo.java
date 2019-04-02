package com.maddy.kafkalearn.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String , String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello from Java producer");

        // send data - asynchronous
        kafkaProducer.send(producerRecord);

        // flush data
        kafkaProducer.flush();

        // or flush data and close producer
        kafkaProducer.close();

    }
}
