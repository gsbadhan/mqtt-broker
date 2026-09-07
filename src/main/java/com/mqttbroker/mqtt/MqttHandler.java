package com.mqttbroker.mqtt;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.springframework.stereotype.Component;

@Component
public class MqttHandler extends SimpleChannelInboundHandler<MqttMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {

        MqttMessageType messageType = message.fixedHeader().messageType();

        System.out.println("Received MQTT message: " + messageType);

        switch (messageType) {

            case CONNECT -> handleConnect(ctx, (MqttConnectMessage) message);

            default -> System.out.println("Unsupported MQTT message: " + messageType);
        }
    }

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage message) {

        String clientId = message.payload().clientIdentifier();

        System.out.println("MQTT CONNECT from client: " + clientId);

        MqttConnAckVariableHeader variableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0);

        MqttConnAckMessage connAck = new MqttConnAckMessage(fixedHeader, variableHeader);

        ctx.writeAndFlush(connAck);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {

        System.out.println("TCP connection established: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {

        System.out.println("TCP connection closed: " + ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        cause.printStackTrace();

        ctx.close();
    }
}