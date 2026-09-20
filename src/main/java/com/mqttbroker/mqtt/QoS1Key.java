package com.mqttbroker.mqtt;

public record QoS1Key(String messageId, String clientId, String topic) {
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(messageId).append(clientId).append(topic);
        return sb.toString();
    }
}
