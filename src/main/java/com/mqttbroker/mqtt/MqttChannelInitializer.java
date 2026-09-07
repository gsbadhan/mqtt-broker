package com.mqttbroker.mqtt;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import org.springframework.stereotype.Component;

@Component
public class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final MqttHandler mqttHandler;
    public MqttChannelInitializer(MqttHandler mqttHandler) {
        this.mqttHandler = mqttHandler;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttHandler", mqttHandler);
    }
}