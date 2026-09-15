package com.mqttbroker.mqtt;

import io.netty.util.AttributeKey;

public class MqttAttributes {
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("clientId");
    public static final AttributeKey<String> DEVICE_ID = AttributeKey.valueOf("deviceId");
    public static final String TYPE = "type";
    public static final String MESSAGE_ID = "messageId";
    public static final String CHALLENGE = "CHALLENGE";
    public static final String QUESTION = "question";
    public static final String ANSWER = "answer";
    public static final String CHALLENGE_TOPIC = "$system/challenge";
}
