package com.mqttbroker.kafka;

import com.mqttbroker.config.KafkaProducerConfig;
import com.mqttbroker.config.KafkaProperties;
import com.mqttbroker.mqtt.ConfirmedMqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    private KafkaProducerConfig producerConfig;
    private KafkaTemplate<String, ConfirmedMqttMessage> confirmedMqttMessageKafkaTemplate;
    private KafkaProperties kafkaProperties;

    @Autowired
    public Producer(KafkaProperties kafkaProperties, KafkaProducerConfig producerConfig,
                    @Qualifier("confirmedMqttMessageKafkaTemplate") KafkaTemplate<String, ConfirmedMqttMessage> confirmedMqttMessageKafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.producerConfig = producerConfig;
        this.confirmedMqttMessageKafkaTemplate = confirmedMqttMessageKafkaTemplate;
    }

    /**
     * Enqueue confirmed MQTT message for next level processing.
     *
     * @param message
     */
    public void publish(ConfirmedMqttMessage message) {
        if(kafkaProperties.getTopics().get(producerConfig.getConfirmedMqttMessageTopic()).getProducer().isEnabled()) {
            log.info("kafka publish clientId={}, packetId={}, topic={}", message.getClientId(), message.getPacketId(),
                    message.getTopic());
            confirmedMqttMessageKafkaTemplate.send(this.producerConfig.getConfirmedMqttMessageTopic(), "00", message);
        }
    }
}
