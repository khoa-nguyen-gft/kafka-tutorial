package com.kafka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {

//		SpringApplication.run(KafkaApplication.class, args);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
//        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "prod-1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<>(producerProps);
//        producer.initTransactions();
//        producer.beginTransaction();
        for (int i = 0; i <10; i ++){
            System.out.println("Sending i -->" + i);
            producer.send(new ProducerRecord<>("topicIn3", String.valueOf(i), "Simple Message-T1-" + i));
//            producer.commitTransaction();

        }

        // IMPORTANCE
        producer.flush();
        producer.close();
    }

}
