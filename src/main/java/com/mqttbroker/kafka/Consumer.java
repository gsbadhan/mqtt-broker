package com.mqttbroker.kafka;

import com.mqttbroker.config.KafkaProperties;
import com.mqttbroker.mqtt.ConfirmedMqttMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true", matchIfMissing = false)
public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @KafkaListener(topics = "${kafka.topics.confirmed-mqtt-messages.name}",
            containerFactory = "confirmedMqttMessagesListenerFactory",
            autoStartup = "${kafka.topics.confirmed-mqtt-messages.consumer.enabled:false}"
    )
    public void consume(ConsumerRecord<String, ConfirmedMqttMessage> record) {
        //TODO
    }
}
