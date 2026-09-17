package com.mqttbroker.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.List;

@Configuration
@ConditionalOnProperty(name = "cache.off-memory.enabled", havingValue = "true")
public class RedisConfig {
    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);
    @Value("${cache.off-memory.redis.standalone.host}")
    private String standaloneHost;
    @Value("${cache.off-memory.redis.standalone.port}")
    private int standalonePort;
    @Value("${cache.off-memory.redis.cluster.nodes}")
    private List<String> nodes;

    @Bean
    @ConditionalOnProperty(name = "cache.off-memory.redis.standalone.enabled", havingValue = "true")
    public LettuceConnectionFactory standaloneConnectionFactory() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(standaloneHost, standalonePort);
        log.info("redis standalone connection factory host={}, port={}", standaloneHost, standalonePort);
        return new LettuceConnectionFactory(configuration);
    }

    @Bean
    @ConditionalOnProperty(name = "cache.off-memory.redis.cluster.enabled", havingValue = "true")
    public LettuceConnectionFactory clusterConnectionFactory() {
        RedisClusterConfiguration configuration = new RedisClusterConfiguration(nodes);
        log.info("redis cluster connection factory hosts={}", nodes);
        return new LettuceConnectionFactory(configuration);
    }

}
