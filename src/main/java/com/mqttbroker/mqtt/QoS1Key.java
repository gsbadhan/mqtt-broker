package com.mqttbroker.mqtt;

public record QoS1Key(String messageId) {
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(messageId);
        return sb.toString();
    }
}
