package com.github.ita.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        String bootstrapServer = "127.0.0.1:9092";

        // create producer property
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // create producer
        Producer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {

            // create record
            ProducerRecord<String, String> record
                    = new ProducerRecord<>("first_topic", "hello world"+i);

            // send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                if(e == null){
                    logger.info("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n " +
                            "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset:" + recordMetadata.offset() + "\n" +
                            "TimeStamp:" + recordMetadata.timestamp()
                    );
                }else{
                    logger.error("Error while producing", e);
                }
            });
        }
        // for see result on console
        // flush data from producer
        producer.flush();

        // flush and close producer
        producer.close();
    }

}
