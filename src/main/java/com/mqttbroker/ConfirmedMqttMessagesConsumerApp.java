package com.mqttbroker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
public class ConfirmedMqttMessagesConsumerApp {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ConfirmedMqttMessagesConsumerApp.class);
        // command line arguments to avoid loading extra beans
        app.setDefaultProperties(Map.of(
                "spring.application.name", "ConfirmedMqttMessagesConsumerApp",
                "mqtt.enabled", "false",
                "mqtt.security.tls.enabled", "false",
                "mqtt.security.challenge.enabled", "false"
        ));
        app.run(args);
    }
}
