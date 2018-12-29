package com.data.report.service.impl;

import com.data.report.domain.DailyActiveUserReportData;
import com.data.report.dto.DauDTO;
import com.data.report.service.ReportDailyActiveUserDao;
import com.data.report.service.ReportDataRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Service
public class ReportDailyActiveUserDaoImpl implements ReportDailyActiveUserDao {

    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String VISITOR_ID_KEYWORD = "visitorId.keyword";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private static final Log logger = LogFactory.getLog(ReportDailyActiveUserDaoImpl.class);


    @Autowired
    private Client client;

    @Autowired
    ReportDataRepository dataRepository;


    public List<DauDTO> byCurrent30Day(){

        // 1. build the query
        QueryBuilder qb = boolQuery()
                .must(rangeQuery(EVENT_TIMESTAMP).gte("now-30d/d").lte("now"));

        // 2. build the aggregations
        AggregationBuilder dau =
                AggregationBuilders
                        .dateHistogram("dau")
                        .field(EVENT_TIMESTAMP)
                        .dateHistogramInterval(DateHistogramInterval.DAY);

        dau.subAggregation(AggregationBuilders.cardinality("distinct_visitors").field(VISITOR_ID_KEYWORD));

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dau)
                .execute().actionGet();


        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalDateHistogram dateHistogram =  (InternalDateHistogram)results.get("dau");

        List<DauDTO> keys = dateHistogram.getBuckets()
                .stream()
                .map(b -> {
                    String key = b.getKeyAsString();
                    InternalCardinality cardinality = (InternalCardinality)b.getAggregations().asMap().get("distinct_visitors");
                    long count = cardinality.getValue();
                    return new DauDTO(count, key);
                })
                .collect(toList());


        keys.forEach(key -> {
            System.out.println(key.getKey());
        });

        return keys;
    }

    private long  byCurrentDay(LocalDateTime dateTime) {

        QueryBuilder qb ;
        if (dateTime != null) {
            LocalDateTime lte = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
            LocalDateTime gte = dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
            qb = boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte(gte.format(dateTimeFormatter)).lte(lte.format(dateTimeFormatter)));
        } else {
            qb =  boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte("now/d").lte("now"));
        }

        // 2. build the aggregations
        AggregationBuilder dau =
                AggregationBuilders.cardinality("dau").field(VISITOR_ID_KEYWORD);

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dau)
                .execute().actionGet();


        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalCardinality cardinality =  (InternalCardinality)results.get("dau");

        return cardinality.getValue();
    }

    private long byCurrentWeek(LocalDateTime dateTime) {

        QueryBuilder qb ;
        if (dateTime != null) {

            LocalDateTime lte = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
            LocalDateTime gte = dateTime.minusDays(7).withHour(0).withMinute(0).withSecond(0).withNano(0);

            qb = boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte(gte.format(dateTimeFormatter)).lte(lte.format(dateTimeFormatter)));
        } else {
            qb = boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte("now-7d/d").lte("now"));
        }

        // 2. build the aggregations
        AggregationBuilder dau =
                AggregationBuilders.cardinality("wau").field(VISITOR_ID_KEYWORD);

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dau)
                .execute().actionGet();


        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalCardinality cardinality =  (InternalCardinality)results.get("wau");

        return cardinality.getValue();
    }

    private long byCurrentMonth(LocalDateTime dateTime) {

        QueryBuilder qb ;
        if (dateTime != null) {

            LocalDateTime lte = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
            LocalDateTime gte = dateTime.minusDays(30).withHour(0).withMinute(0).withSecond(0).withNano(0);

            qb = boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte(gte.format(dateTimeFormatter)).lte(lte.format(dateTimeFormatter)));

        } else {
            qb = boolQuery()
                    .must(rangeQuery(EVENT_TIMESTAMP).gte("now-30d/d").lte("now"));
        }

        // 2. build the aggregations
        AggregationBuilder dau =
                AggregationBuilders.cardinality("mau").field(VISITOR_ID_KEYWORD);

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dau)
                .execute().actionGet();


        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalCardinality cardinality =  (InternalCardinality)results.get("mau");

        return cardinality.getValue();
    }

    @Override
    public void report(LocalDateTime dateTime) {

        logger.info("Report "+ dateTime);
        long day = byCurrentDay(dateTime);
        long week = byCurrentWeek(dateTime);
        long month = byCurrentMonth(dateTime);

        DailyActiveUserReportData data = new DailyActiveUserReportData();
        data.setDay(dateTime.format(dateFormatter));

        data.setDau(day);
        data.setWau(week);
        data.setMau(month);

        dataRepository.save(data);

        //System.out.println(String.format("day: %s - week: %s - month: %s", day, week, month));
    }

    @Override
    public void reportAll(LocalDateTime from) {
        LocalDateTime to = LocalDateTime.now().plusDays(1), fDate = from;
        do {
            report(fDate);
            fDate = fDate.plusDays(1);
        } while (fDate.isBefore(to));
    }
}
