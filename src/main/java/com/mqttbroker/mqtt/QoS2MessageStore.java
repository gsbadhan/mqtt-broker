package com.mqttbroker.mqtt;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
public class QoS2MessageStore {
    private final Cache<QoS2Key, QoS2Message> messages =
            Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(Duration.ofMinutes(5)).build();

    public void put(QoS2Message message) {
        QoS2Key key = new QoS2Key(message.getClientId(), message.getPacketId());
        messages.put(key, message);
    }

    public QoS2Message get(String clientId, int packetId) {
        return messages.getIfPresent(new QoS2Key(clientId, packetId));
    }

    public QoS2Message remove(String clientId, int packetId) {
        messages.invalidate(new QoS2Key(clientId, packetId));
        return null;
    }

    public boolean contains(String clientId, int packetId) {
        QoS2Message qoS2Message = messages.getIfPresent(new QoS2Key(clientId, packetId));
        if (qoS2Message != null) {
            return true;
        }
        return false;
    }
}
