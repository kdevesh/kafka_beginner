package com.github.kdevesh.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {
    public static void main(String[] args) {
        String bootStrapServer = "localhost:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            String topic = "first_topic";
            String value = RandomString.getAlphaNumericString(6);
            String key = "id_"+i;
            //create a Producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: "+key);
            //send data -> asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                //executes every time a record is successfully sent or qn exception is thrown
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "SerializedValueSize: " + recordMetadata.serializedValueSize() + "\n" +
                                "SerializedKeySize: " + recordMetadata.serializedKeySize());
                    } else
                        logger.error("Exception occurred while producing:", e);
                }
            });
        }
        //flush data
        kafkaProducer.flush();
        //flush and close producer
        kafkaProducer.close();
    }
}
