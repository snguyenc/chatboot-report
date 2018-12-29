package com.data.report.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "ReportActiveUser")
public class DailyActiveUserReportData {

    @Id
    public String day;

    public long dau;
    public long wau;
    public long mau;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public long getDau() {
        return dau;
    }

    public void setDau(long dau) {
        this.dau = dau;
    }

    public long getWau() {
        return wau;
    }

    public void setWau(long wau) {
        this.wau = wau;
    }

    public long getMau() {
        return mau;
    }

    public void setMau(long mau) {
        this.mau = mau;
    }
}
