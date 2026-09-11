package com.mqttbroker.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqttbroker.kafka.Producer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final SubscriptionManager subscriptionManager;
    private final QoS1MessageStore qoS1MessageStore;
    private final QoS2MessageStore qos2MessageStore;
    private final Producer producer;
    private final ObjectMapper objectMapper;
    private final SslContext mqttSslContext;


    @Autowired
    public MqttChannelInitializer(SubscriptionManager subscriptionManager, QoS2MessageStore qos2MessageStore,
                                  Producer producer, QoS1MessageStore qoS1MessageStore, ObjectMapper objectMapper, SslContext mqttSslContext) {
        this.subscriptionManager = subscriptionManager;
        this.qos2MessageStore = qos2MessageStore;
        this.producer = producer;
        this.qoS1MessageStore = qoS1MessageStore;
        this.objectMapper = objectMapper;
        this.mqttSslContext = mqttSslContext;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("ssl", mqttSslContext.newHandler(channel.alloc()));
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttHandler", new MqttHandler(subscriptionManager, qos2MessageStore, producer,
                qoS1MessageStore, objectMapper));
    }
}