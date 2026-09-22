package com.mqttbroker.config;

public class TopicProperties {
    private String name;
    private ProducerProperties producer;
    private ConsumerProperties consumer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ProducerProperties getProducer() {
        return producer;
    }

    public void setProducer(ProducerProperties producer) {
        this.producer = producer;
    }

    public ConsumerProperties getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerProperties consumer) {
        this.consumer = consumer;
    }
}
