package com.mqttbroker.mqtt;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);

    private final SubscriptionManager subscriptionManager;

    @Autowired
    public MqttHandler(SubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
        MqttMessageType messageType = message.fixedHeader().messageType();
        log.info("Received MQTT message messageType={}", messageType);

        switch (messageType) {
            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
            case PINGREQ -> handlePingReq(ctx);
            case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnSubscribe(ctx, (MqttSubscribeMessage) message);
            case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
            case DISCONNECT -> handleDisconnect(ctx);
            default -> log.info("Unsupported MQTT message messageType={}", messageType);
        }
    }

    private void handleUnSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = message.variableHeader().messageId();
        log.info("MQTT UNSUBSCRIBE received clientId={} packetId={}", clientId, packetId);
        for (MqttTopicSubscription topicFilter : message.payload().topicSubscriptions()) {
            log.info("MQTT unsubscribe clientId={} topicFilter={}", clientId, topicFilter);
            subscriptionManager.removeSubscription(clientId, topicFilter.topicFilter());
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttUnsubAckMessage unsubAck = new MqttUnsubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(packetId));
        ctx.writeAndFlush(unsubAck);
        log.info("MQTT UNSUBACK sent clientId={} packetId={}", clientId, packetId);
    }

    private void handleDisconnect(ChannelHandlerContext ctx) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        log.info("MQTT DISCONNECT received clientId={}", clientId);
        ctx.close();
        subscriptionManager.removeClient(clientId);
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        String topic = message.variableHeader().topicName();
        String payload = message.payload().toString(StandardCharsets.UTF_8);
        MqttQoS qos = message.fixedHeader().qosLevel();
        boolean retain = message.fixedHeader().isRetain();
        boolean dup = message.fixedHeader().isDup();
        log.info("MQTT PUBLISH received clientId={} topic={} qos={} retain={} dup={} payload={}", clientId, topic, qos, retain, dup, payload);
    }

    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = message.variableHeader().messageId();
        log.info("MQTT SUBSCRIBE received clientId={} packetId={}", clientId, packetId);
        if (clientId == null) {
            log.warn("MQTT SUBSCRIBE received without clientId channel={}", ctx.channel().id().asShortText());
            ctx.close();
            return;
        }
        List<Integer> grantedQos = new ArrayList<>();

        for (MqttTopicSubscription subscription : message.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicName();
            MqttQoS requestedQos = subscription.qualityOfService();
            log.info("MQTT subscription clientId={} topicFilter={} requestedQos={}", clientId, topicFilter, requestedQos);
            subscriptionManager.addSubscription(clientId, topicFilter);
            // For now, grant QoS 0
            grantedQos.add(MqttQoS.AT_MOST_ONCE.value());
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQos);
        MqttSubAckMessage subAck = new MqttSubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(packetId), payload);
        ctx.writeAndFlush(subAck);
        log.info("MQTT SUBACK sent clientId={} packetId={} grantedQos={}", clientId, packetId, grantedQos);
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        String clientId = message.payload().clientIdentifier();
        log.info("MQTT CONNECT from client clientId={}", clientId);
        ctx.channel().attr(MqttAttributes.CLIENT_ID).set(clientId);
        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckMessage connAck = new MqttConnAckMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(connAck);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("TCP connection established {}", ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("TCP connection closed {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private void handlePingReq(ChannelHandlerContext ctx) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage pingResp = new MqttMessage(fixedHeader);
        ctx.writeAndFlush(pingResp);
        log.info("MQTT PINGREQ received clientId={} sent PINGRESP", clientId);
    }
}