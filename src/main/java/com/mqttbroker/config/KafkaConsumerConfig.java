package com.mqttbroker.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "kafka.enabled", havingValue = "true")
public class KafkaConsumerConfig {
    private final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean("consumerFactoriesByTopic")
    public Map<String, ConsumerFactory<String, Object>> consumerFactoriesByTopic() {
        Map<String, ConsumerFactory<String, Object>> factories = new HashMap<>();
        kafkaProperties.getTopics().forEach((alias, topicProp) -> {
            Map<String, Object> configs = new HashMap<>();
            configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
            configs.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties.getClientId());
            // Per-topic config
            ConsumerProperties c = topicProp.getConsumer();
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, c.getGroupId());
            configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, c.getAutoOffsetReset());
            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, c.getKeyDeserializer());
            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, c.getValueDeserializer());
            configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, c.getEnableAutoCommit());
            configs.put(KafkaAttributes.AUTO_STARTUP, c.isEnabled());
            factories.put(alias, new DefaultKafkaConsumerFactory<>(configs));
            log.info("loaded consumer factory topic={}, configs={}", topicProp.getName(), configs);
        });
        return factories;
    }

    @Bean("confirmedMqttMessagesListenerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> confirmedMqttMessagesListenerFactory(@Qualifier("consumerFactoriesByTopic") Map<String, ConsumerFactory<String, Object>> cfMap) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        ConsumerFactory cf = cfMap.get("confirmed-mqtt-messages");
        factory.setConsumerFactory(cf);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setAutoStartup((Boolean) cf.getConfigurationProperties().get(KafkaAttributes.AUTO_STARTUP));
        return factory;
    }
}
