package com.data.report.service;

import com.data.report.domain.UserSessionReportData;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SessionUserRepository extends MongoRepository<UserSessionReportData, String> {



}
