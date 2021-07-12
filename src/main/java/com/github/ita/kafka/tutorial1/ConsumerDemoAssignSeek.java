package com.github.ita.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offSetToReadFrom = 15L;
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);

        int numberOfMessageRead = 5;
        boolean keepReading = true;
        int messageOfReadSoFar = 0;

        // poll for new data
        while(keepReading){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records) {
                messageOfReadSoFar += 1;
                logger.info("Key: " + record.key() + ", ValueL: " + record.value());
                logger.info("partition: " + record.partition() + ", Offset: " + record.offset());
                if (messageOfReadSoFar >= numberOfMessageRead) {
                    keepReading = false; // to exit the while loop
                    break; // to exit the for loop
                }
            }
        }
        logger.info("exiting the application");
    }

}
