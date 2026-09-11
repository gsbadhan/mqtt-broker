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
     *
     * @param message
     */
    public void publish(ConfirmedMqttMessage message) {
        //producer.submit(message)
        log.info("kafka publish clientId={}, packetId={}, topic={}", message.getClientId(), message.getPacketId(),
                message.getTopic());

    }
}
