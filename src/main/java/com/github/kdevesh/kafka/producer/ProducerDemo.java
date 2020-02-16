package com.github.kdevesh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootStrapServer = "localhost:9092";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //create a Producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first_topic","Kafka in Java");

        //send data -> asynchronous
        kafkaProducer.send(producerRecord);
        //flush data
        kafkaProducer.flush();
        //flush and close producer
        kafkaProducer.close();
    }
}
