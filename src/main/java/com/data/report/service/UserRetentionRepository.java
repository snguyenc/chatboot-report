package com.data.report.service;

import com.data.report.domain.UserRetentionReportData;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface UserRetentionRepository extends MongoRepository<UserRetentionReportData, String> {



}
