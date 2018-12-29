package com.data.report.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "ReportUserSession")
public class UserSessionReportData {

    @Id
    public String day;

    public long dailySession;
    public long dailySessionPerUser;
    public double sessionTimePerUser;
    public double messagePerSession;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public long getDailySession() {
        return dailySession;
    }

    public void setDailySession(long dailySession) {
        this.dailySession = dailySession;
    }

    public long getDailySessionPerUser() {
        return dailySessionPerUser;
    }

    public void setDailySessionPerUser(long dailySessionPerUser) {
        this.dailySessionPerUser = dailySessionPerUser;
    }

    public double getSessionTimePerUser() {
        return sessionTimePerUser;
    }

    public void setSessionTimePerUser(double sessionTimePerUser) {
        this.sessionTimePerUser = sessionTimePerUser;
    }

    public double getMessagePerSession() {
        return messagePerSession;
    }

    public void setMessagePerSession(double messagePerSession) {
        this.messagePerSession = messagePerSession;
    }
}
