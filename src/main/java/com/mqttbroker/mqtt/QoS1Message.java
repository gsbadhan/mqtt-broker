package com.mqttbroker.mqtt;

public class QoS1Message {
    private final String messageId;
    private final String clientId;
    private final int packetId;
    private final String topic;

    public QoS1Message(String messageId, String clientId, int packetId, String topic) {
        this.messageId = messageId;
        this.clientId = clientId;
        this.packetId = packetId;
        this.topic = topic;
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

}
