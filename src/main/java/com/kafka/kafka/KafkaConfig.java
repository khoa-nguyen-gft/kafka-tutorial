package com.kafka.kafka;

public enum KafkaConfig {
    TOPIC("topicIn4"),
    BOOTSTRAP_SERVERS("localhost:9092"),
    GROUP_ID("my-fourth-application"),
    EARLY("earliest");

    private String value;

    KafkaConfig(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
