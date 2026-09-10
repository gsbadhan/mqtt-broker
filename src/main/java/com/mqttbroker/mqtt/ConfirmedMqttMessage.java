package com.mqttbroker.mqtt;

public class ConfirmedMqttMessage {
    private final String messageId;
    private final String clientId;
    private final int packetId;
    private final String topic;
    private final byte[] payload;


    public ConfirmedMqttMessage(String messageId, String clientId, int packetId, String topic, byte[] payload) {
        this.messageId = messageId;
        this.clientId = clientId;
        this.packetId = packetId;
        this.topic = topic;
        this.payload = payload;
    }

    public String getMessageId() {
        return messageId;
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
