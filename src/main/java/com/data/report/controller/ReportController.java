package com.data.report.controller;

import com.data.report.domain.DailyActiveUserReportData;
import com.data.report.service.ReportDailyActiveUserDao;
import com.data.report.service.ReportUserRetentionDao;
import com.data.report.service.ReportUserSessionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@RestController
public class ReportController {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE;

    @Autowired
    private ReportDailyActiveUserDao reportDailyActiveUserDao;

    @Autowired
    private ReportUserRetentionDao reportUserRetentionDao;

    @Autowired
    private ReportUserSessionDao reportUserSessionDao;

    @GetMapping("/reports/all/index")
    public Boolean otpRequestIndex(){

        LocalDateTime localDateTime = LocalDateTime.now().minusDays(30);
        reportDailyActiveUserDao.reportAll(localDateTime);
        reportUserSessionDao.reportAll(localDateTime);
        reportUserRetentionDao.reportAll(localDateTime);

        return Boolean.TRUE;
    }

}
