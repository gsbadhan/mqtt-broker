package com.mqttbroker.mqtt;

public record QoS2Key(String clientId, int packetId) {
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(clientId).append(clientId).append(packetId);
        return sb.toString();
    }
}
