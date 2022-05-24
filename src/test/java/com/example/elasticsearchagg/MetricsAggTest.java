package com.example.elasticsearchagg;

import com.example.elasticsearchagg.service.MetricsAggregationsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author dengR
 * @date 2022/4/27 20:31
 */
@SpringBootTest
public class MetricsAggTest {

    @Autowired
    private MetricsAggregationsService service;

    @Test
    public void testAvg() {
        service.AvgAggregations();
    }

    @Test
    public void testCardinality() {
        service.CardinalityAggregations();
    }

}
