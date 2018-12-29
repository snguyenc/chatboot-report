package com.data.report.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ReportJob {

    @Autowired
    ReportUserSessionDao userSessionDao;

    @Autowired
    ReportDailyActiveUserDao dailyActiveUserDao;

    @Autowired
    ReportUserRetentionDao reportUserRetentionDao;

    @Scheduled(fixedRateString = "${cron.timer}")
    public void scheduleTaskWithFixedRate() {

        LocalDateTime now = LocalDateTime.now();

        dailyActiveUserDao.report(now);
        userSessionDao.report(now);
        reportUserRetentionDao.report(now);
    }
}
