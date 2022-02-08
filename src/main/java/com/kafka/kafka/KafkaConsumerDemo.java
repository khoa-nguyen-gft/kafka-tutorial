package com.kafka.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static com.kafka.kafka.KafkaConfig.BOOTSTRAP_SERVERS;
import static com.kafka.kafka.KafkaConfig.EARLY;
import static com.kafka.kafka.KafkaConfig.GROUP_ID;
import static com.kafka.kafka.KafkaConfig.TOPIC;


public class KafkaConsumerDemo {


    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS.value());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID.value());
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLY.value());

        KafkaConsumer consumer = new KafkaConsumer(properties);

        consumer.subscribe(Arrays.asList(TOPIC.value()));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                logger.info("Keys: " + record.key());
                logger.info("Value: " + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("offset: " + record.offset());
            }
        }
    }
}
