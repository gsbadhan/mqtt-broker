package com.mqttbroker.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mqttbroker.cache.CacheManager;
import com.mqttbroker.kafka.Producer;
import com.mqttbroker.security.ValidationInterceptor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;

public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final SubscriptionManager subscriptionManager;
    private final ObjectMapper objectMapper;
    private final Producer producer;
    private ValidationInterceptor validationInterceptor;
    private CacheManager cache;


    public MqttHandler(SubscriptionManager subscriptionManager, Producer producer, ObjectMapper objectMapper, ValidationInterceptor validationInterceptor, CacheManager cache) {
        this.subscriptionManager = subscriptionManager;
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.validationInterceptor = validationInterceptor;
        this.cache = cache;
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
            case PUBACK -> handlePublishAck(ctx, (MqttPubAckMessage) message);
            case PUBREL -> handlePubRel(ctx, message);
            case DISCONNECT -> handleDisconnect(ctx);
            default -> log.info("Unsupported MQTT messageType={}", messageType);
        }
    }

    private void handlePublishAck(ChannelHandlerContext ctx, MqttPubAckMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        MqttQoS qos = message.fixedHeader().qosLevel();
        int packetId = message.variableHeader().messageId();
        log.info("MQTT PUBACK received clientId={}, packetId={}, qos={}", clientId, packetId, qos);
        //TODO: any further action
    }

    private void handlePubRel(ChannelHandlerContext ctx, MqttMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = ((MqttMessageIdVariableHeader) message.variableHeader()).messageId();
        QoS2Message qos2Message = cache.qoS2Message.get(new QoS2Key(clientId, packetId).toString());
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
        cache.qoS2Message.delete(new QoS2Key(clientId, packetId).toString());
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
        log.info("MQTT PUBLISH QoS2 received clientId={}, packetId={}, topic={}, qos={}", clientId, packetId, topic,
                qos);
        if (!challengeFlow(ctx, message.fixedHeader().messageType(), clientId, packetId, topic,
                null)) {
            return;
        }
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        /*
         * Check whether this packet already exists.
         */
        if (cache.qoS2Message.contains(new QoS2Key(clientId, packetId).toString())) {
            log.info("Duplicate MQTT PUBLISH QoS2 received clientId={}, packetId={}", clientId, packetId);
            sendPubRec(ctx, packetId);
            return;
        }
        /*
         * Store the message.
         */
        QoS2Message qos2Message = new QoS2Message(clientId, packetId, topic, payloadBytes);
        cache.qoS2Message.put(new QoS2Key(clientId, packetId).toString(), qos2Message);
        log.info("MQTT PUBLISH QoS2 message stored clientId={}, packetId={}, topic={}", clientId, packetId, topic);

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
        String messageId, responseType;
        String topic = message.variableHeader().topicName();
        int packetId = message.variableHeader().packetId();
        ByteBuf payload = message.payload();
        log.info("MQTT PUBLISH QoS1 received clientId={}, packetId={}, topic={}, qos={}", clientId, packetId, topic,
                qos);
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        try {
            JsonNode jsonNode = objectMapper.readTree(payloadBytes);
            messageId = jsonNode.path(MqttAttributes.MESSAGE_ID).asText(null);
            if (messageId == null || messageId.isBlank()) {
                log.warn("Missing messageId clientId={}, packetId={}, topic={}", clientId, packetId, topic);
                sendPubNack(ctx, clientId, packetId, topic, MqttPublishError.PAYLOAD_FORMAT_INVALID);
                return;
            }
            if (!challengeFlow(ctx, message.fixedHeader().messageType(), clientId, packetId, topic,
                    jsonNode)) {
                return;
            }
        } catch (IOException e) {
            log.error("error occurred while parsing message clientId={} packetId={}, error={}", clientId, packetId, e);
            sendPubNack(ctx, clientId, packetId, topic, MqttPublishError.PAYLOAD_FORMAT_INVALID);
            return;
        }
        log.info("MQTT PUBLISH QoS1 clientId={},packetId={}, messageId={}, topic={}", clientId, packetId, messageId,
                topic);

        // its duplicate and already in processing queue
        if (cache.qoS1Message.contains(new QoS1Key(messageId, clientId, topic).toString())) {
            sendPubNack(ctx, clientId, packetId, topic, MqttPublishError.DUPLICATE_MESSAGE);
            return;
        }
        producer.publish(new ConfirmedMqttMessage(null, clientId, packetId, topic, payloadBytes));
        cache.qoS1Message.put(new QoS1Key(messageId, clientId, topic).toString(), new QoS1Message(messageId, clientId
                , packetId, topic));
        sendPubAck(ctx, clientId, packetId, topic, MqttPublishError.SUCCESS);
    }

    private void sendPubAck(ChannelHandlerContext ctx, String clientId, int packetId,
                            String topic, MqttPublishError error) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = new MqttPubReplyMessageVariableHeader(packetId, error.getCode(),
                MqttProperties.NO_PROPERTIES);
        MqttMessage pubAck = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubAck);
        log.info("MQTT PUBACK sent clientId={}, packetId={}, topic={}, error={}", clientId, packetId,
                topic, error);
    }

    private void sendPubNack(ChannelHandlerContext ctx, String clientId, int packetId,
                             String topic, MqttPublishError error) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = new MqttPubReplyMessageVariableHeader(packetId, error.getCode(),
                MqttProperties.NO_PROPERTIES);
        MqttMessage pubAck = new MqttMessage(fixedHeader, variableHeader);
        ctx.writeAndFlush(pubAck);
        log.info("MQTT PUBACK sent clientId={}, packetId={}, topic={}, error={}", clientId, packetId,
                topic, error);
    }


    /*
     * PUBLISH -> Nothing
     */
    private void handlePublishQoS0(String clientId, ChannelHandlerContext ctx, MqttPublishMessage message, MqttQoS qos) {
        String topic = message.variableHeader().topicName();
        int packetId = message.variableHeader().messageId();
        ByteBuf payload = message.payload();
        log.info("MQTT PUBLISH QoS0 received clientId={} packetId={} topic={} qos={}", clientId, packetId, topic, qos);
        if (!challengeFlow(ctx, message.fixedHeader().messageType(), clientId, packetId, topic,
                null)) {
            return;
        }
        byte[] payloadBytes = new byte[payload.readableBytes()];
        payload.getBytes(payload.readerIndex(), payloadBytes);
        producer.publish(new ConfirmedMqttMessage(null, clientId, packetId, topic, payloadBytes));
    }


    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage message) {
        String clientId = ctx.channel().attr(MqttAttributes.CLIENT_ID).get();
        int packetId = message.variableHeader().messageId();
        log.info("MQTT SUBSCRIBE received clientId={}, packetId={}", clientId, packetId);
        if (clientId == null) {
            log.warn("MQTT SUBSCRIBE received without clientId channel={}", ctx.channel().id().asShortText());
            ctx.close();
            return;
        }
        MqttTopicSubscription subscription = message.payload().topicSubscriptions().get(0);
        String topicFilter = subscription.topicName();
        if (!challengeFlow(ctx, message.fixedHeader().messageType(), clientId, packetId, topicFilter,
                null)) {
            return;
        }
        MqttQoS requestedQos = subscription.qualityOfService();
        log.info("MQTT subscription clientId={}, packetId={}, topicFilter={}, requestedQos={}", clientId, packetId, topicFilter,
                requestedQos);
        subscriptionManager.addSubscription(clientId, topicFilter);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(MqttQoS.AT_MOST_ONCE.value());
        MqttSubAckMessage subAck = new MqttSubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(packetId), payload);
        ctx.writeAndFlush(subAck);
        log.info("MQTT SUBACK sent clientId={}, packetId={}, topicFilter={}, grantedQos={}", clientId, packetId,
                topicFilter,
                MqttQoS.AT_MOST_ONCE);
        challengeFlow(ctx, message.fixedHeader().messageType(), clientId, packetId, topicFilter, null);
    }

    private boolean challengeFlow(ChannelHandlerContext ctx, MqttMessageType messageType, String clientId, int packetId,
                                  String topic, JsonNode jsonPayload) {
        Optional<String> challenge = validationInterceptor.checkChallenge(clientId);
        boolean shouldContinue = false;
        if (challenge.isEmpty()) {
            shouldContinue = true;
            return shouldContinue;
        }
        if (!topic.equals(MqttAttributes.CHALLENGE_TOPIC)) {
            shouldContinue = false;
            sendPubNack(ctx, clientId, packetId, topic, MqttPublishError.NOT_AUTHORIZED);
            return shouldContinue;
        }
        switch (messageType) {
            case SUBSCRIBE -> {
                sendChallengeMessage(ctx, clientId, packetId, challenge.get(), topic);
            }
            case PUBLISH -> {
                String responseType = jsonPayload.path(MqttAttributes.TYPE).asText(null);
                if (responseType == null || !responseType.equals(MqttAttributes.CHALLENGE)) {
                    sendPubAck(ctx, clientId, packetId, topic, MqttPublishError.PAYLOAD_FORMAT_INVALID);
                    break;
                }
                String question = jsonPayload.path(MqttAttributes.QUESTION).asText(null);
                String answer = jsonPayload.path(MqttAttributes.ANSWER).asText(null);
                String messageId = jsonPayload.path(MqttAttributes.MESSAGE_ID).asText(null);
                boolean isPassed = validationInterceptor.verifyChallenge(clientId, question, answer);
                if (isPassed) {
                    sendPubAck(ctx, clientId, packetId, topic, MqttPublishError.SUCCESS);
                    Optional<String> nextChallenge = validationInterceptor.checkChallenge(clientId);
                    if (nextChallenge.isPresent()) {
                        sendChallengeMessage(ctx, clientId, packetId, nextChallenge.get(), topic);
                    }
                } else {
                    sendPubNack(ctx, clientId, packetId, topic, MqttPublishError.NOT_AUTHORIZED);
                }
            }
        }
        return shouldContinue;
    }

    private void sendChallengeMessage(ChannelHandlerContext ctx, String clientId, int packetId, String challenge,
                                      String topic) {
        String messageId = "msg-" + System.currentTimeMillis();
        String payload = """
                 {
                  "type": "%s",
                  "messageId": "%s",
                  "question": "%s"
                 }
                """.formatted(MqttAttributes.CHALLENGE, messageId, challenge);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topic, packetId);
        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, variableHeader,
                Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8));
        ctx.writeAndFlush(publishMessage);
        log.info("MQTT CHALLENGE sent clientId={}, packetId={}, topic={}", clientId, packetId, topic);
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {
        String clientId = message.payload().clientIdentifier();
        log.info("MQTT CONNECT from client clientId={}", clientId);
        if (isTlsConnection(ctx) && !deviceSecurityCheck(ctx, clientId)) {
            return;
        }
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

    private boolean isTlsConnection(ChannelHandlerContext ctx) {
        return ctx.pipeline().get(SslHandler.class) != null;
    }

    private boolean deviceSecurityCheck(ChannelHandlerContext ctx, String clientId) {
        String deviceId = getDeviceId(ctx, clientId);
        if (deviceId == null) {
            log.warn("No device identity found for clientId={}", clientId);
            ctx.close();
            return false;
        }
        if (!deviceId.equals(clientId)) {
            log.warn("Device identity mismatch: certificateDeviceId={}, clientId={}", deviceId, clientId);
            ctx.close();
            return false;
        }
        log.info("Device authenticated: deviceId={}, clientId={}", deviceId, clientId);
        ctx.channel().attr(MqttAttributes.DEVICE_ID).set(deviceId);
        return true;
    }

    private String getDeviceId(ChannelHandlerContext ctx, String clientId) {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            return null;
        }
        try {
            SSLSession session = sslHandler.engine().getSession();
            Certificate[] certificates = session.getPeerCertificates();
            if (certificates.length == 0) {
                log.error("No certificates found for clientId={} !!", clientId);
                return null;
            }
            X509Certificate clientCertificate = (X509Certificate) certificates[0];
            return extractDeviceId(clientCertificate);
        } catch (SSLPeerUnverifiedException e) {
            log.warn("Unable to verify client certificate for clientId={}, error={}", clientId, e);
            return null;
        }
    }

    private String extractDeviceId(X509Certificate certificate) {
        String subject = certificate.getSubjectX500Principal().getName();
        for (String part : subject.split(",")) {
            String[] keyValue = part.trim().split("=", 2);
            if (keyValue.length == 2 && keyValue[0].equalsIgnoreCase("CN")) {
                return keyValue[1];
            }
        }
        return null;
    }
}