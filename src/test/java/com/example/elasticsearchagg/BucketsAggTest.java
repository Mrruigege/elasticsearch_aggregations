package com.example.elasticsearchagg;

import com.example.elasticsearchagg.service.BucketsAggregationsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author dengR
 * @date 2022/4/27 20:31
 */
@SpringBootTest
public class BucketsAggTest {

    @Autowired
    private BucketsAggregationsService service;

    @Test
    public void testDateHistogram() {
        service.dateHistogramAgg();
    }

    @Test
    public void testDateRangeAgg() {
        service.dateRangeAgg();
    }

    @Test
    public void testSignificantTermsAgg() {
        service.significantTermsAgg();
    }
}
