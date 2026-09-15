package com.mqttbroker.security;

public class ChallengeData {
    private String question;
    private String answer;
    private boolean isCorrect;

    public ChallengeData(String question, String answer, boolean isCorrect) {
        this.question = question;
        this.answer = answer;
        this.isCorrect = isCorrect;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public boolean isCorrect() {
        return isCorrect;
    }

    public void setCorrect(boolean correct) {
        isCorrect = correct;
    }
}
