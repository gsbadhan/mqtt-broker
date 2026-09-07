package com.mqttbroker.mqtt;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SubscriptionManager {
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();
    public void addSubscription(String clientId, String topicFilter) {
        subscriptions.computeIfAbsent(clientId, key -> ConcurrentHashMap.newKeySet()).add(topicFilter);
    }

    public Set<String> getSubscriptions(String clientId) {
        return subscriptions.getOrDefault(clientId, Set.of());
    }

    public void removeClient(String clientId) {
        subscriptions.remove(clientId);
    }
}
