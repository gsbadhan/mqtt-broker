package com.mqttbroker.mqtt;

public enum MqttPublishError {
    SUCCESS((byte) 0x00),
    DUPLICATE_MESSAGE((byte) 0x83),
    NOT_AUTHORIZED((byte) 0x87),
    TOPIC_NAME_INVALID((byte) 0x90),
    QUOTA_EXCEEDED((byte) 0x97),
    PAYLOAD_FORMAT_INVALID((byte) 0x99);

    private final byte code;

    MqttPublishError(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }
}
