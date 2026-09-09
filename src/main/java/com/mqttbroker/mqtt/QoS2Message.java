package com.mqttbroker.mqtt;

public class QoS2Message {
    private final String clientId;
    private final int packetId;
    private final String topic;
    private final byte[] payload;

    public QoS2Message(String clientId, int packetId, String topic, byte[] payload) {
        this.clientId = clientId;
        this.packetId = packetId;
        this.topic = topic;
        this.payload = payload;
    }

    public String getClientId() {
        return clientId;
    }

    public int getPacketId() {
        return packetId;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }
}
