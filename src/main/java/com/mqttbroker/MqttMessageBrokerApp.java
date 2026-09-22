package com.mqttbroker;

import com.mqttbroker.config.KafkaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties.class)
public class MqttMessageBrokerApp {
    public static void main(String[] args) {
        SpringApplication.run(MqttMessageBrokerApp.class, args);
    }

}
