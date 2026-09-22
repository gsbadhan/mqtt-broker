package com.mqttbroker.config;

import com.mqttbroker.mqtt.ConfirmedMqttMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
public class KafkaProducerConfig {
    private final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);
    private String confirmedMqttMessageTopic;
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean("confirmedMqttMessageKafkaTemplate")
    public KafkaTemplate<String, ConfirmedMqttMessage> confirmedMqttMessageKafkaTemplate(@Value("${kafka.topics.confirmed-mqtt-messages.name}") String confirmedMqttMessageTopic) {
        ProducerProperties props = kafkaProperties.getTopics().get(confirmedMqttMessageTopic).getProducer();
        this.confirmedMqttMessageTopic = confirmedMqttMessageTopic;
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperties.getClientId());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getKeySerializer());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getValueSerializer());
        configs.put(ProducerConfig.ACKS_CONFIG, props.getAcks());
        configs.put(ProducerConfig.RETRIES_CONFIG, props.getRetries());
        DefaultKafkaProducerFactory kafkaProducerFactory = new DefaultKafkaProducerFactory<>(configs);
        log.info("confirmedMqttMessageKafkaTemplate topic={}, configs={}", this.confirmedMqttMessageTopic, configs);
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    public String getConfirmedMqttMessageTopic() {
        return confirmedMqttMessageTopic;
    }
}
