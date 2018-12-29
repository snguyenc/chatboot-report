package com.data.report.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Document(collection = "ReportUserRetention")
public class UserRetentionReportData {

    @Id
    public String day;
    private long count;

    public List<DailyRetention> retentions = new ArrayList<>();


    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public List<DailyRetention> getRetentions() {
        return retentions;
    }

    public void setRetentions(List<DailyRetention> retentions) {
        this.retentions = retentions;
    }

    public void newDailyRetention(String day, long count) {
        DailyRetention dailyRetention = new DailyRetention();
        dailyRetention.setDay(day);
        dailyRetention.setCount(count);
        this.retentions.add(dailyRetention);
    }

    public void updateDailyRetention(String day, long count) {
        DailyRetention lastEl;
        if (this.retentions.isEmpty()) {
            lastEl = new DailyRetention();
            lastEl.setDay(day);
            lastEl.setCount(count);
        } else {
            lastEl = this.retentions.get(this.retentions.size() - 1);
            if (lastEl.getDay().equals(day)) {
                lastEl.setCount(count);
            } else {
                lastEl = new DailyRetention();
                lastEl.setDay(day);
                lastEl.setCount(count);
            }
        }
        this.retentions.add(lastEl);
    }

    private class DailyRetention {
        private String day;
        private long count;

        public String getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = day;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }
}
