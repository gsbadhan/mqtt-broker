package com.mqttbroker.mqtt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class MqttServer {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);
    private final MqttChannelInitializer channelInitializer;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    @Value("${mqtt.broker.host}")
    private String host;

    @Value("${mqtt.broker.port}")
    private int port;

    @Value("${mqtt.broker.boss-threads}")
    private int bossThreads;

    @Value("${mqtt.broker.worker-threads}")
    private int workerThreads;

    public MqttServer(MqttChannelInitializer channelInitializer) {
        this.channelInitializer = channelInitializer;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(channelInitializer).childOption(ChannelOption.SO_KEEPALIVE, true);
        serverChannel = bootstrap.bind(host, port).sync().channel();
        log.info("MQTT Broker started on host={},port={}", host, port);
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}