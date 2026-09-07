package com.mqttbroker.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SubscriptionManager {
    private static final Logger log = LoggerFactory.getLogger(MqttServer.class);

    // clientId -> topic filters
    private final Map<String, Set<String>> subscriptions = new ConcurrentHashMap<>();

    // topic filter -> clientIds
    private final Map<String, Set<String>> subscribers = new ConcurrentHashMap<>();

    public void addSubscription(String clientId, String topicFilter) {
        // clientId -> topics
        subscriptions.computeIfAbsent(clientId, key -> ConcurrentHashMap.newKeySet()).add(topicFilter);

        // topic -> clients
        subscribers.computeIfAbsent(topicFilter, key -> ConcurrentHashMap.newKeySet()).add(clientId);
    }

    public Set<String> getSubscriptions(String clientId) {
        return subscriptions.getOrDefault(clientId, Set.of());
    }

    public Set<String> getSubscribers(String topic) {
        return subscribers.getOrDefault(topic, Set.of());
    }

    public void removeClient(String clientId) {
        Set<String> clientSubscriptions = subscriptions.remove(clientId);
        log.info("subscriptions for clientId={}",clientId);
        if (clientSubscriptions == null) {
            return;
        }
        for (String topicFilter : clientSubscriptions) {
            Set<String> clients = subscribers.get(topicFilter);
            log.info("subscriptions for clientId={},topicFilter={}",clientId,topicFilter);
            if (clients != null) {
                clients.remove(clientId);
                if (clients.isEmpty()) {
                    subscribers.remove(topicFilter);
                }
            }
        }
    }

    public void removeSubscription(String clientId, String topicFilter) {
        Set<String> clientSubscriptions = subscriptions.get(clientId);
        log.info("subscriptions for clientId={},topicFilter={}",clientId,topicFilter);
        if (clientSubscriptions != null) {
            clientSubscriptions.remove(topicFilter);
            if (clientSubscriptions.isEmpty()) {
                subscriptions.remove(clientId);
            }
        }

        Set<String> clients = subscribers.get(topicFilter);
        log.info("clients for clientId={},topicFilter={}",clientId,topicFilter);
        if (clients != null) {
            clients.remove(clientId);
            if (clients.isEmpty()) {
                subscribers.remove(topicFilter);
            }
        }
    }
}
