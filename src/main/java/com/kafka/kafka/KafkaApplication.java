package com.kafka.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KafkaApplication {

    private static final Logger log = LoggerFactory.getLogger(KafkaApplication.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

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
        for (int i = 0; i < 50; i++) {
            System.out.println("Sending i -->" + i);

            String topic = "topicIn4";
            String message = "Simple Message-T1-" + i;
            String keyMessage = "id_" + String.valueOf(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, keyMessage, message);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Receive metadata: \n" +
                                "topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()

                        );
                    }
                }
            });
//            producer.commitTransaction();

        }

        // IMPORTANCE
        producer.flush();
        producer.close();
    }

}
