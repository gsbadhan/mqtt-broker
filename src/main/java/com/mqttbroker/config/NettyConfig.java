package com.mqttbroker.config;

import com.mqttbroker.mqtt.MqttServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NettyConfig {

    @Bean
    @ConditionalOnProperty(name = "mqtt.enabled", havingValue = "true")
    CommandLineRunner startMqttServer(MqttServer mqttServer) {
        return args -> mqttServer.start();
    }
}