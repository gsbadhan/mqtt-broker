package com.mqttbroker.mqtt;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class QoS2MessageStore {
    private final Map<QoS2Key, QoS2Message> messages = new ConcurrentHashMap<>();

    public void put(QoS2Message message) {
        QoS2Key key = new QoS2Key(message.getClientId(), message.getPacketId());
        messages.put(key, message);
    }

    public QoS2Message get(String clientId, int packetId) {
        return messages.get(new QoS2Key(clientId, packetId));
    }

    public QoS2Message remove(String clientId, int packetId) {
        return messages.remove(new QoS2Key(clientId, packetId));
    }

    public boolean contains(String clientId, int packetId) {
        return messages.containsKey(new QoS2Key(clientId, packetId));
    }
}
