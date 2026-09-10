package com.mqttbroker.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqttbroker.kafka.Producer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);

    private final SubscriptionManager subscriptionManager;
    private final QoS1MessageStore qos1MessageStore;
    private final QoS2MessageStore qos2MessageStore;
    private final ObjectMapper objectMapper;
    private final Producer producer;


    public MqttHandler(SubscriptionManager subscriptionManager, QoS2MessageStore qos2MessageStore, Producer producer,
                       QoS1MessageStore qos1MessageStore, ObjectMapper objectMapper) {
        this.subscriptionManager = subscriptionManager;
        this.qos2MessageStore = qos2MessageStore;
        this.producer = producer;
        this.qos1MessageStore = qos1MessageStore;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
        MqttMessageType messageType = message.fixedHeader().messageType();
        log.info("Received MQTT message messageType={}", messageType);

        switch (messageType) {
            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);
            case PINGREQ -> handlePingReq(ctx);
            case SUBSCRIBE -> handleSubscribe(ctx, (MqttSubscribeMessage) message);
            case UNSUBSCRIBE -> handleUnSubscribe(ctx, (MqttUnsubscribeMessage) message);
            case PUBLISH -> handlePublish(ctx, (MqttPublishMessage) message);
            case PUBREL -> handlePubRel(ctx, message);
            case DISCONNECT -> handleDisconnect(ctx);
            default -> log.info("Unsupported MQTT messageType={}", messageType);
        }
    }

    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
        QoS2Message qos2Message = qos2MessageStore.get(clientId, packetId);
        if (qos2Message == null) {
            log.warn("MQTT PUBREL QoS2 received but QoS2 message not found clientId={} packetId={}", clientId, packetId);
            sendPubComp(ctx, packetId);
            return;
        }
        /*
         * NOW process the message.
         */
        producer.publish(new ConfirmedMqttMessage(null, qos2Message.getClientId(), qos2Message.getPacketId(),
                qos2Message.getTopic(), qos2Message.getPayload()));
        /*
         * Remove QoS2 state.
         */
        qos2MessageStore.remove(clientId, packetId);
        /*
         * Complete QoS2 handshake.
         */
        sendPubComp(ctx, packetId);
    }

    private void sendPubComp(ChannelHandlerContext ctx, int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
        MqttMessage pubComp = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubComp);
        log.info("MQTT PUBCOMP QoS2 sent packetId={}", packetId);
    }

    /*
    UNSUBSCRIBE -> UNSUBACK
     */
    private void handleUnSubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = message.variableHeader().messageId();
        log.info("MQTT UNSUBSCRIBE received clientId={} packetId={}", clientId, packetId);
        for (String topicFilter : message.payload().topics()) {
            log.info("MQTT UNSUBSCRIBE clientId={} topicFilter={}", clientId, topicFilter);
            subscriptionManager.removeSubscription(clientId, topicFilter);
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
        MqttQoS qos = message.fixedHeader().qosLevel();
        switch (qos) {
            case AT_MOST_ONCE:
                handlePublishQoS0(clientId, ctx, message, qos);
                break;
            case AT_LEAST_ONCE:
                handlePublishQoS1(clientId, ctx, message, qos);
                break;
            case EXACTLY_ONCE:
                handlePublishQoS2(clientId, ctx, message, qos);
                break;
        }
    }

    /*
     * PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
     */
    private void handlePublishQoS2(String clientId, ChannelHandlerContext ctx, MqttPublishMessage message, MqttQoS qos) {
        String topic = message.variableHeader().topicName();
        int packetId = message.variableHeader().packetId();
        ByteBuf payload = message.payload().asByteBuf();
        log.info("MQTT PUBLISH QoS2 received clientId={} packetId={} topic={} qos={}", clientId, packetId, topic, qos);
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        /*
         * Check whether this packet already exists.
         */
        if (qos2MessageStore.contains(clientId, packetId)) {
            log.info("Duplicate MQTT PUBLISH QoS2 received clientId={} packetId={}", clientId, packetId);
            sendPubRec(ctx, packetId);
            return;
        }
        /*
         * Store the message.
         */
        QoS2Message qos2Message = new QoS2Message(clientId, packetId, topic, payloadBytes);
        qos2MessageStore.put(qos2Message);
        log.info("MQTT PUBLISH QoS2 message stored clientId={} packetId={} topic={}", clientId, packetId, topic);

        /*
         * Tell the publisher that the broker
         * has received the PUBLISH.
         */
        sendPubRec(ctx, packetId);
    }

    private void sendPubRec(ChannelHandlerContext ctx, int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
        MqttMessage pubRec = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubRec);
        log.info("MQTT PUBREC sent packetId={}", packetId);
    }


    private void handlePublishQoS1(String clientId, ChannelHandlerContext ctx, MqttPublishMessage message, MqttQoS qos) {
        String messageId;
        String topic = message.variableHeader().topicName();
        int packetId = message.variableHeader().packetId();
        ByteBuf payload = message.payload();
        log.info("MQTT PUBLISH QoS1 received clientId={} packetId={} topic={} qos={}", clientId, packetId, topic, qos);
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        try {
            JsonNode jsonNode = objectMapper.readTree(payloadBytes);
            messageId = jsonNode.path(MqttAttributes.MESSAGE_ID).asText(null);
            if (messageId == null || messageId.isBlank()) {
                log.warn("Missing messageId clientId={} packetId={} topic={}", clientId, packetId, topic);
                sendPubNack(ctx, packetId, MqttPublishError.PAYLOAD_FORMAT_INVALID);
                return;
            }
        } catch (IOException e) {
            log.error("error occurred while parsing message clientId={} packetId={}, error={}", clientId, packetId, e);
            sendPubNack(ctx, packetId, MqttPublishError.PAYLOAD_FORMAT_INVALID);
            return;
        }
        log.info("MQTT PUBLISH QoS1 clientId={} packetId={} messageId={} topic={}", clientId, packetId, messageId, topic);

        // its duplicate and already in processing queue
        if (qos1MessageStore.contains(messageId, clientId, topic)) {
            sendPubNack(ctx, packetId, MqttPublishError.DUPLICATE_MESSAGE);
            return;
        }
        producer.publish(new ConfirmedMqttMessage(null, clientId, packetId, topic, payloadBytes));
        sendPubAck(ctx, packetId);
    }

    private void sendPubAck(ChannelHandlerContext ctx, int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(packetId);
        MqttMessage pubAck = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubAck);
        log.info("MQTT PUBACK sent packetId={}", packetId);
    }

    private void sendPubNack(ChannelHandlerContext ctx, int packetId, MqttPublishError error) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = new MqttPubReplyMessageVariableHeader(packetId, error.getCode(),
                MqttProperties.NO_PROPERTIES);
        MqttMessage pubAck = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubAck);
        log.info("MQTT PUBACK sent with error={}, packetId={}", error, packetId);
    }


    /*
     * PUBLISH -> Nothing
     */
    private void handlePublishQoS0(String clientId, ChannelHandlerContext ctx, MqttPublishMessage message, MqttQoS qos) {
        String topic = message.variableHeader().topicName();
        int packetId = message.variableHeader().messageId();
        ByteBuf payload = message.payload();
        log.info("MQTT PUBLISH QoS0 received clientId={} packetId={} topic={} qos={}", clientId, packetId, topic, qos);
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        producer.publish(new ConfirmedMqttMessage(null, clientId, packetId, topic, payloadBytes));
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
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
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