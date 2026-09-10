package com.mqttbroker.kafka;

import com.mqttbroker.mqtt.ConfirmedMqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    /**
     * Enqueue message for next level processing.
     * @param message
     */
    public void publish(ConfirmedMqttMessage message) {
    // Message ready to publish in Kafka processing
    }
}
