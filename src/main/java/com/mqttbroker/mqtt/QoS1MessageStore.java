package com.mqttbroker.mqtt;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class QoS1MessageStore {
    private final Cache<QoS1Key, QoS1Message> messages =
            Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(Duration.ofMinutes(5)).build();

    public void put(QoS1Message message) {
        QoS1Key key = new QoS1Key(message.getMessageId(), message.getClientId(), message.getTopic());
        messages.put(key, message);
    }

    public QoS1Message get(String messageId, String clientId, String topic) {
        return messages.getIfPresent(new QoS1Key(messageId, clientId, topic));
    }

    public QoS1Message remove(String messageId, String clientId, String topic) {
        messages.invalidate(new QoS1Key(messageId, clientId, topic));
        return null;
    }

    public boolean contains(String messageId, String clientId, String topic) {
        QoS1Message qoS1Message = messages.getIfPresent(new QoS1Key(messageId, clientId, topic));
        if (qoS1Message != null) {
            return true;
        }
        return false;
    }
}
