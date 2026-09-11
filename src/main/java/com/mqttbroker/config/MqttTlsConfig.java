package com.mqttbroker.config;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLException;
import java.io.File;

@Component
public class MqttTlsConfig {
    private static final Logger log = LoggerFactory.getLogger(MqttTlsConfig.class);
    @Value("${mqtt.tls.server-cert}")
    private String serverCert;

    @Value("${mqtt.tls.server-key}")
    private String serverKey;

    @Value("${mqtt.tls.device-ca}")
    private String deviceCa;

    @Bean
    @ConditionalOnProperty(name = "mqtt.tls.enabled", havingValue = "true", matchIfMissing = false)
    public SslContext mqttSslContext() throws SSLException {
        log.info("loading TLS..");
        return SslContextBuilder.forServer(new File(serverCert), new File(serverKey)).trustManager(new File(deviceCa)).clientAuth(ClientAuth.REQUIRE).build();
    }
}
