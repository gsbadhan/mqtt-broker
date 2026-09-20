package com.mqttbroker.security;

import com.mqttbroker.cache.CacheManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class DeviceChallenge {
    private final Logger log = LoggerFactory.getLogger(DeviceChallenge.class);
    @Value("${mqtt.security.challenge.load-dummy-challenges}")
    private boolean loadDummyChallenges;

    private CacheManager cache;

    @Autowired
    public DeviceChallenge(CacheManager cache) {
        this.cache = cache;
    }

    @PostConstruct
    public void loadDummyChallenges() {
        if (loadDummyChallenges) {
            log.info("loading dummy challenges..");
            String device1 = "device-001";
            String question1 = "add these two numbers 2 and 3";
            String answer1 = "5";
            String device2 = "device-002";
            String question2 = "multiply these two numbers 7 and 8";
            String answer2 = "56";

            loadChallenges(device1, question1, answer1);
            loadChallenges(device1, question2, answer2);
            updateChallengeStatus(device1, false);

            loadChallenges(device2, question1, answer1);
            loadChallenges(device2, question2, answer2);
            updateChallengeStatus(device2, false);
        }
    }

    public void loadChallenges(String deviceId, String question, String answer) {
        List<ChallengeData> list = cache.challengeStore.get(deviceId);
        if (list == null) {
            list = new ArrayList<>(3);
        }
        list.add(new ChallengeData(question, answer, false));
        cache.challengeStore.put(deviceId, list);
    }

    public Optional<ChallengeData> getChallenge(String deviceId, String question) {
        List<ChallengeData> list = cache.challengeStore.get(deviceId);
        for (ChallengeData challenge : list) {
            if (challenge.getQuestion().equals(question)) {
                return Optional.of(challenge);
            }
        }
        return Optional.empty();
    }

    public Optional<ChallengeData> getPendingChallenge(String deviceId) {
        if (cache.challengeStatus.get(deviceId) != null && cache.challengeStatus.get(deviceId).booleanValue()) {
            return Optional.empty();
        }
        List<ChallengeData> list = cache.challengeStore.get(deviceId);
        for (ChallengeData challenge : list) {
            if (!challenge.isCorrect()) {
                return Optional.of(challenge);
            }
        }
        return Optional.empty();
    }


    public boolean validateResponse(String expectedAnswer, String response) {
        return expectedAnswer.equalsIgnoreCase(response);
    }

    public void updateChallenge(String deviceId, String question, boolean isCorrect) {
        List<ChallengeData> list = cache.challengeStore.get(deviceId);
        for (ChallengeData challenge : list) {
            if (challenge.getQuestion().equals(question)) {
                challenge.setCorrect(isCorrect);
            }
        }
        cache.challengeStore.put(deviceId, list);
        if (isChallengePassed(deviceId)) {
            cache.challengeStatus.put(deviceId, true);
        }
    }

    public boolean isChallengePassed(String deviceId) {
        List<ChallengeData> list = cache.challengeStore.get(deviceId);
        for (ChallengeData challenge : list) {
            if (!challenge.isCorrect()) {
                return false;
            }
        }
        return true;
    }

    public void updateChallengeStatus(String deviceId, boolean status) {
        cache.challengeStatus.put(deviceId, status);
    }
}
