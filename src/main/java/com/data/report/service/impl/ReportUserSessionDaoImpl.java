package com.data.report.service.impl;

import com.data.report.domain.UserSessionReportData;
import com.data.report.service.ReportDataRepository;
import com.data.report.service.ReportUserSessionDao;
import com.data.report.service.SessionUserRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

@Service
public class ReportUserSessionDaoImpl implements ReportUserSessionDao {

    public static final String EVENT_TIMESTAMP = "event_timestamp";
    public static final String VISITOR_ID_KEYWORD = "visitorId.keyword";
    public static final String VISITOR_SESSION_ID_KEYWORD = "visitorSessionId.keyword";
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private static final Log logger = LogFactory.getLog(ReportUserSessionDaoImpl.class);


    @Autowired
    private Client client;

    @Autowired
    ReportDataRepository dataRepository;

    @Autowired
    SessionUserRepository sessionUserRepository;


    @Override
    public void report(LocalDateTime dateTime) {
        logger.info("Report "+ dateTime);
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
        AggregationBuilder dsession =
                AggregationBuilders.cardinality("dsession").field(VISITOR_SESSION_ID_KEYWORD);

        // 2. build the aggregations
        AggregationBuilder duser =
                AggregationBuilders.cardinality("duser").field(VISITOR_ID_KEYWORD);


        // 2. build the aggregations
        AggregationBuilder dsessionPerUser =
                AggregationBuilders.terms("dsessionPerUser").field(VISITOR_SESSION_ID_KEYWORD);

        Script initScript = new Script("state.dates=[];");
        Script mapScript = new Script("state.dates.add(doc.event_timestamp.value.getMillis());");
        Script compineScript = new Script("def min = state.dates[0]; def max = state.dates[0]; for (t in state.dates) { min = min < t ? min:t ; max = max > t ? max:t} return [min, max]");
        Script redurceScript = new Script("def min = states[0][0]; def max = states[0][1]; for (t in states) { min = min < t[0] ? min:t[0] ; max = max > t[1] ? max:t[1]} return (max -min)/1000 ;");
        // 2. build the aggregations
        AggregationBuilder dsessionPerUserSub =
                AggregationBuilders.scriptedMetric("duration").initScript(initScript).mapScript(mapScript).combineScript(compineScript).reduceScript(redurceScript);
        dsessionPerUser.subAggregation(dsessionPerUserSub);

        AvgBucketPipelineAggregationBuilder avgSessionPU =
                PipelineAggregatorBuilders.avgBucket("avg_mili", "dsessionPerUser>duration.value");


        AvgBucketPipelineAggregationBuilder avgSessionPerMessage =
                PipelineAggregatorBuilders.avgBucket("avg_message", "dsessionPerUser>_count");



        // 3. make the query
        SearchResponse response = client.prepareSearch()
                .setSize(0)
                .setQuery(qb)
                .addAggregation(dsession)
                .addAggregation(duser)
                .addAggregation(dsessionPerUser)
                .addAggregation(avgSessionPU)
                .addAggregation(avgSessionPerMessage)
                .execute().actionGet();


        Map<String, Aggregation> results = response.getAggregations().asMap();
        InternalCardinality cardinalityDsession =  (InternalCardinality)results.get("dsession");
        InternalCardinality cardinalityDuser =  (InternalCardinality)results.get("duser");
        InternalSimpleValue avgMili =  (InternalSimpleValue)results.get("avg_mili");
        InternalSimpleValue avgMessage =  (InternalSimpleValue)results.get("avg_message");

        UserSessionReportData data = new UserSessionReportData();
        data.setDay(dateTime.toLocalDate().toString());

        data.setDailySession(cardinalityDsession.getValue());
        if (cardinalityDuser.getValue() > 0) {
            data.setDailySessionPerUser(cardinalityDsession.getValue()/cardinalityDuser.getValue());
        }
        data.setSessionTimePerUser(avgMili.getValue());
        data.setMessagePerSession(avgMessage.getValue());

        sessionUserRepository.save(data);
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
