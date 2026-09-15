package com.mqttbroker.security;

import com.mqttbroker.mqtt.MqttAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class ValidationInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ValidationInterceptor.class);
    private final DeviceChallenge deviceChallenge;
    @Value("${mqtt.security.challenge.enabled}")
    private boolean challengeEnabled;

    public ValidationInterceptor(DeviceChallenge deviceChallenge) {
        this.deviceChallenge = deviceChallenge;
        if (challengeEnabled) {
            log.info("DeviceChallenge enabled={}", deviceChallenge);
        }
    }

    public Optional<String> checkChallenge(String deviceId) {
        if (!challengeEnabled) {
            return Optional.empty();
        }
        Optional<ChallengeData> challenge = deviceChallenge.getPendingChallenge(deviceId);
        if (challenge.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(challenge.get().getQuestion());
    }

    public boolean verifyChallenge(String deviceId, String question, String response) {
        Optional<ChallengeData> challenge = deviceChallenge.getChallenge(deviceId, question);
        if (challenge.isPresent()) {
            boolean status = deviceChallenge.validateResponse(challenge.get().getAnswer(), response);
            if (status) {
                deviceChallenge.updateChallenge(deviceId, question, status);
                return true;
            }
        }
        return false;
    }

}
