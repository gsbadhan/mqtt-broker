package com.mqttbroker.mqtt;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final SubscriptionManager subscriptionManager;
    private final QoS2MessageStore qos2MessageStore;

    @Autowired
    public MqttChannelInitializer(SubscriptionManager subscriptionManager, QoS2MessageStore qos2MessageStore) {
        this.subscriptionManager = subscriptionManager;
        this.qos2MessageStore = qos2MessageStore;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttHandler", new MqttHandler(subscriptionManager, qos2MessageStore));
    }
}