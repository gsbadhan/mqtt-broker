package com.mqttbroker.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqttbroker.cache.CacheManager;
import com.mqttbroker.kafka.Producer;
import com.mqttbroker.security.DeviceChallenge;
import com.mqttbroker.security.ValidationInterceptor;
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
    private final Producer producer;
    private final ObjectMapper objectMapper;
    private final SslContext mqttSslContext;
    private ValidationInterceptor validationInterceptor;
    private CacheManager cache;


    @Autowired
    public MqttChannelInitializer(SubscriptionManager subscriptionManager, Producer producer,
                                  ObjectMapper objectMapper, SslContext mqttSslContext,
                                  ValidationInterceptor validationInterceptor, CacheManager cache) {
        this.subscriptionManager = subscriptionManager;
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.mqttSslContext = mqttSslContext;
        this.validationInterceptor = validationInterceptor;
        this.cache = cache;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("ssl", mqttSslContext.newHandler(channel.alloc()));
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttHandler", new MqttHandler(subscriptionManager, producer, objectMapper,
                validationInterceptor, cache));
    }
}