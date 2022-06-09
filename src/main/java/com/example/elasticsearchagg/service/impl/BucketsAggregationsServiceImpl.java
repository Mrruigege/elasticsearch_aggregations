package com.example.elasticsearchagg.service.impl;

import cn.hutool.core.date.DateUtil;
import com.example.elasticsearchagg.service.BucketsAggregationsService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @author dengR
 * @date 2022/4/27 20:31
 */
@Service
public class BucketsAggregationsServiceImpl implements BucketsAggregationsService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 固定时间间隔分组进行统计
     */
    @Override
    public void dateHistogramAgg() {
        SearchRequest searchRequest = new SearchRequest("feedback");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.query(QueryBuilders.rangeQuery("createTime").gt("1650340801000"));
        // 根据固定时间进行统计分组
//        sourceBuilder.aggregation(AggregationBuilders.dateHistogram("timeAgg")
//                .field("createTime")
//                .fixedInterval(DateHistogramInterval.DAY)
//                .format("yyyy-MM-dd"));
        // 根据日历时间进行统计分组
        sourceBuilder.aggregation(AggregationBuilders.dateHistogram("timeAgg")
                .field("createTime")
                .calendarInterval(DateHistogramInterval.WEEK)
                .format("yyyy-MM-dd"));
        // 聚合不做查询
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Histogram agg = response.getAggregations().get("timeAgg");
            List<? extends Histogram.Bucket> buckets = agg.getBuckets();
            for (Histogram.Bucket bucket : buckets) {
                System.out.println(bucket.getKeyAsString());
                System.out.println(bucket.getDocCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义时间间隔分组进行统计
     */
    @Override
    public void dateRangeAgg() {
        SearchRequest searchRequest = new SearchRequest("feedback");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.dateRange("timeAgg")
                .field("createTime")
                .addUnboundedTo(DateUtil.parse("2022-4-20").toDateStr())
                .addRange(DateUtil.parse("2022-4-20").toDateStr(), DateUtil.parse("2022-4-25").toDateStr())
                .addUnboundedFrom(DateUtil.parse("2022-4-25").toDateStr())
                .format("yyyy-MM-dd"));
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Range range = response.getAggregations().get("timeAgg");
            List<? extends Range.Bucket> buckets = range.getBuckets();
            for (Range.Bucket bucket : buckets) {
                System.out.println("from time :" + bucket.getFromAsString());
                System.out.println("to time  : " + bucket.getToAsString());
                System.out.println("doc count : " + bucket.getDocCount());
                System.out.println("----------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void significantTermsAgg() {
        SearchRequest searchRequest = new SearchRequest("blank_account");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.query(QueryBuilders.rangeQuery("balance").gte(20000));
        sourceBuilder.aggregation(AggregationBuilders.significantTerms("stProportion")
                .field("state"));
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SignificantTerms agg = response.getAggregations().get("stProportion");
            List<? extends SignificantTerms.Bucket> buckets = agg.getBuckets();
            for (SignificantTerms.Bucket bucket : buckets) {
                System.out.println("分组key：" + bucket.getKeyAsString());
                System.out.println("聚合条件文档数：" + bucket.getDocCount());
                System.out.println("匹配文档数中，符合查询条件的文档数：" + bucket.getSubsetDf());
                System.out.println("总文档数中，忽略查询条件，该分组下的文档数：" + bucket.getSupersetDf());
                System.out.println("符合查询的文档数：" + bucket.getSubsetSize());
                System.out.println("总文档数：" + bucket.getSupersetSize());
                System.out.println("--------------------");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
