package com.mqttbroker.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mqttbroker.mqtt.QoS1Message;
import com.mqttbroker.mqtt.QoS2Message;
import com.mqttbroker.security.ChallengeData;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Component
public class CacheManager {
    @Value("${cache.in-memory.enabled}")
    private boolean isInMemory;

    // deviceId -> ChallengeData (question, answer, isCorrect)
    public KVcache<String, List<ChallengeData>> challengeStore;

    // deviceId -> status (true/false). Challenge passed by device or not.
    public KVcache<String, Boolean> challengeStatus;

    // QoS1Key(String messageId, String clientId, String topic) -> QoS1Message()
    public KVcache<String, QoS1Message> qoS1Message;

    // QoS2Key(String clientId, int packetId) -> QoS2Message()
    public KVcache<String, QoS2Message> qoS2Message;
    private String challengeStoreNs = "challenge-store";
    @Value("${cache.namespaces.challenge-store.max-cache-size}")
    private long challengeStoreMaxSize;
    @Value("${cache.namespaces.challenge-store.key-ttl}")
    private Duration challengeStoreTtl;
    private String challengeStatusNs = "challenge-status";
    @Value("${cache.namespaces.challenge-status.max-cache-size}")
    private long challengeStatusMaxSize;
    @Value("${cache.namespaces.challenge-status.key-ttl}")
    private Duration challengeStatusTtl;
    private String qos1MessageNs = "qos1-message";
    @Value("${cache.namespaces.qos1-message.max-cache-size}")
    private long qos1MessageMaxSize;
    @Value("${cache.namespaces.qos1-message.key-ttl}")
    private Duration qos1MessageTtl;
    private String qos2MessageNs = "qos2-message";
    @Value("${cache.namespaces.qos2-message.max-cache-size}")
    private long qos2MessageMaxSize;
    @Value("${cache.namespaces.qos2-message.key-ttl}")
    private Duration qos2MessageTtl;
    @Autowired
    private RedisTemplate redisTemplate;

    @PostConstruct
    public void init() {
        if (isInMemory) {
            this.challengeStore = new InMemoryKVCache<>(challengeStoreNs, challengeStoreMaxSize, challengeStoreTtl);
            this.challengeStatus = new InMemoryKVCache<>(challengeStatusNs, challengeStatusMaxSize, challengeStatusTtl);
            this.qoS1Message = new InMemoryKVCache<>(qos1MessageNs, qos1MessageMaxSize, qos1MessageTtl);
            this.qoS2Message = new InMemoryKVCache<>(qos2MessageNs, qos2MessageMaxSize, qos2MessageTtl);
        } else {
            this.challengeStore = new OffMemoryKVCache<>(redisTemplate, challengeStoreNs, challengeStoreMaxSize, challengeStoreTtl);
            this.challengeStatus = new OffMemoryKVCache<>(redisTemplate, challengeStoreNs, challengeStoreMaxSize, challengeStoreTtl);
            this.qoS1Message = new OffMemoryKVCache<>(redisTemplate, qos1MessageNs, qos1MessageMaxSize, qos1MessageTtl);
            this.qoS2Message = new OffMemoryKVCache<>(redisTemplate, qos2MessageNs, qos2MessageMaxSize, qos2MessageTtl);
        }
    }
}
