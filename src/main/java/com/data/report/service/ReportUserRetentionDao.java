package com.data.report.service;

import java.time.LocalDateTime;

public interface ReportUserRetentionDao {

    public void report(LocalDateTime dateTime);
    public void reportAll(LocalDateTime from);

}
