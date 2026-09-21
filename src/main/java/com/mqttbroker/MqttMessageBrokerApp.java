package com.mqttbroker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MqttMessageBrokerApp {
    public static void main(String[] args) {
        SpringApplication.run(MqttMessageBrokerApp.class, args);
    }

}
