package com.data.report.service.impl;

import com.data.report.domain.UserRetentionReportData;
import com.data.report.dto.DauDTO;
import com.data.report.service.ReportDataRepository;
import com.data.report.service.ReportUserRetentionDao;
import com.data.report.service.SessionUserRepository;
import com.data.report.service.UserRetentionRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service("reportUserRetentionDao")
public class ReportUserRetentionDaoImpl implements ReportUserRetentionDao {

    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String VISITOR_ID_KEYWORD = "visitorId.keyword";
    public static final String VISITOR_SESSION_ID_KEYWORD = "visitorSessionId.keyword";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;

    private static final Log logger = LogFactory.getLog(ReportUserRetentionDaoImpl.class);

    @Autowired
    private Client client;


    @Autowired
    UserRetentionRepository userRetentionRepository;


    private long getNewUser(LocalDateTime dateTime){

        LocalDateTime lte = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
        LocalDateTime gte = dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);

        QueryBuilder qb = boolQuery()
                .filter(rangeQuery(EVENT_TIMESTAMP).gte(gte.format(dateTimeFormatter)).lte(lte.format(dateTimeFormatter))).filter(termQuery("user_event.keyword",
                        "visitor-first-time-use"));

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .execute().actionGet();


        SearchHits results = response.getHits();
        return results.getTotalHits();
    }

    private List<DauDTO> getRetentionUser(LocalDateTime dateTime){

        LocalDateTime lte = dateTime.withHour(23).withMinute(59).withSecond(59).withNano(999);
        LocalDateTime gte = dateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);

        QueryBuilder qb = boolQuery().mustNot(termQuery("user_event.keyword","visitor-first-time-use"))
                .filter(rangeQuery(EVENT_TIMESTAMP).gte(gte.format(dateTimeFormatter)).lte(lte.format(dateTimeFormatter)))
                .filter(existsQuery("first_time_use"));

        // 2. build the aggregations
        AggregationBuilder dateRetention =
                AggregationBuilders
                        .dateHistogram("dateRetention")
                        .field("first_time_use")
                        .format("yyyy-MM-dd")
                        .dateHistogramInterval(DateHistogramInterval.DAY);

        dateRetention.subAggregation(AggregationBuilders.cardinality("visistorCnt").field(VISITOR_ID_KEYWORD));

        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dateRetention)
                .execute().actionGet();

        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalDateHistogram dateHistogram =  (InternalDateHistogram)results.get("dateRetention");

        List<DauDTO> keys = dateHistogram.getBuckets()
                .stream()
                .map(b -> {
                    String key = b.getKeyAsString();
                    InternalCardinality cardinality = (InternalCardinality)b.getAggregations().asMap().get("visistorCnt");
                    long count = cardinality.getValue();
                    //System.out.println(key + ":" + count);
                    return new DauDTO(count, key);
                })
                .collect(toList());

        return keys;

    }

    @Override
    public void report(LocalDateTime dateTime) {

        String currDay = dateTime.format(dateFormatter);
        logger.info("Report "+ dateTime);
        long newUser = getNewUser(dateTime);
        UserRetentionReportData data = new UserRetentionReportData();
        data.setDay(currDay);
        data.setCount(newUser);
        data.newDailyRetention(currDay, newUser);
        userRetentionRepository.save(data);

        //get all retention user

        List<DauDTO> keys = getRetentionUser(dateTime);
        keys.stream().forEach(k -> {
            if (k.getCount() > 0) {
                String date = k.getKey().replaceAll("T00:00:00.*", "");
                if (!date.equals(currDay)) {
                    Optional<UserRetentionReportData> retentionRepositoryById = userRetentionRepository.findById(date);
                    if (retentionRepositoryById.isPresent()) {
                        retentionRepositoryById.get().updateDailyRetention(currDay, k.getCount());
                        userRetentionRepository.save(retentionRepositoryById.get());
                    }
                }
            }
        });

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
