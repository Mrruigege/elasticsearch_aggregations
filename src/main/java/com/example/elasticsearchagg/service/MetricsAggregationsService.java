package com.example.elasticsearchagg.service;

/**
 * 指标聚合
 * @author dengR
 * @date 2022/4/27 20:31
 */
public interface MetricsAggregationsService {

    void AvgAggregations();

    void CardinalityAggregations();
}
