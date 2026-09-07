package com.mqttbroker.config;

import com.mqttbroker.mqtt.MqttServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NettyConfig {

    @Bean
    CommandLineRunner startMqttServer(MqttServer mqttServer) {
        return args -> mqttServer.start();
    }
}