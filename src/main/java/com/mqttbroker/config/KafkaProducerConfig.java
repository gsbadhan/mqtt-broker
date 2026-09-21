package com.mqttbroker.config;

import com.mqttbroker.mqtt.ConfirmedMqttMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
public class KafkaProducerConfig {
    private final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.client-id}")
    private String clientId;
    private String confirmedMqttMessageTopic;

    @Bean("confirmedMqttMessageKafkaTemplate")
    public KafkaTemplate<String, ConfirmedMqttMessage> confirmedMqttMessageKafkaTemplate(@Value("${kafka.topics.confirmed-mqtt-messages.name}")
                                                                                         String confirmedMqttMessageTopic,
                                                                                         @Value("${kafka.topics.confirmed-mqtt-messages.producer.key-serializer}")
                                                                                         String confirmedMqttMessageKeySerializer,
                                                                                         @Value("${kafka.topics.confirmed-mqtt-messages.producer.value-serializer}")
                                                                                         String confirmedMqttMessageValueSerializer,
                                                                                         @Value("${kafka.topics.confirmed-mqtt-messages.producer.acks}")
                                                                                         String confirmedMqttMessageAcks,
                                                                                         @Value("${kafka.topics.confirmed-mqtt-messages.producer.retries}")
                                                                                         int confirmedMqttMessageRetries) {
        this.confirmedMqttMessageTopic = confirmedMqttMessageTopic;
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, confirmedMqttMessageKeySerializer);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, confirmedMqttMessageValueSerializer);
        configs.put(ProducerConfig.ACKS_CONFIG, confirmedMqttMessageAcks);
        configs.put(ProducerConfig.RETRIES_CONFIG, confirmedMqttMessageRetries);
        DefaultKafkaProducerFactory kafkaProducerFactory = new DefaultKafkaProducerFactory<>(configs);
        log.info("confirmedMqttMessageKafkaTemplate configs={}", configs);
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    public String getConfirmedMqttMessageTopic() {
        return confirmedMqttMessageTopic;
    }
}
