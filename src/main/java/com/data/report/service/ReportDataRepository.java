package com.data.report.service;

import com.data.report.domain.DailyActiveUserReportData;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ReportDataRepository extends MongoRepository<DailyActiveUserReportData, String> {



}
