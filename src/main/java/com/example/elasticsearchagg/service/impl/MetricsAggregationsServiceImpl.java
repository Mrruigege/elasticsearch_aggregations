package com.example.elasticsearchagg.service.impl;

import com.example.elasticsearchagg.service.MetricsAggregationsService;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author dengR
 * @date 2022/4/27 20:31
 */
@Service
public class MetricsAggregationsServiceImpl implements MetricsAggregationsService {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 计算指定字段的平均值
     */
    @Override
    public void AvgAggregations() {
        SearchRequest searchRequest = new SearchRequest("goods");
        ;
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.avg("priceAvg")
                .field("price")
                .missing(10));
        // 设置为0，将不返回查询的数据
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Avg avg = response.getAggregations().get("priceAvg");
            System.out.println(avg.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 计算指定字段不同值的个数，相当于distinct
     */
    @Override
    public void CardinalityAggregations() {
        SearchRequest searchRequest = new SearchRequest("goods");
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.aggregation(AggregationBuilders.cardinality("areaCardinality")
                .field("attributionArea.keyword")
                .precisionThreshold(10000));
        // 设置为0，将不返回查询的数据
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Cardinality cardinality = response.getAggregations().get("areaCardinality");
            System.out.println(cardinality.getValue());
            System.out.println(cardinality.getValueAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SearchRequest buildSearchRequest() {
        return new SearchRequest("goods");
    }
}
