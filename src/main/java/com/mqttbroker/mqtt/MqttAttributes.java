package com.mqttbroker.mqtt;

import io.netty.util.AttributeKey;

public class MqttAttributes {
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("clientId");
    public static final String MESSAGE_ID = "messageId";
}
