package com.mqttbroker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    private boolean enabled;
    private String bootstrapServers;
    private String clientId;
    private KafkaConnectionProperties properties;
    private Map<String, TopicProperties> topics = new HashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public KafkaConnectionProperties getProperties() {
        return properties;
    }

    public void setProperties(KafkaConnectionProperties properties) {
        this.properties = properties;
    }

    public Map<String, TopicProperties> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicProperties> topics) {
        this.topics = topics;
    }
}
