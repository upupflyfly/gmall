package com.atguigu.gmall1018.dw.publisher.com.atguigu.gmall1018.dw.publisher.service.impl;

import com.atguigu.gmall1018.dw.common.constant.GmallConstant;
import com.atguigu.gmall1018.dw.publisher.com.atguigu.gmall1018.dw.publisher.service.PublishService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishServiceImpl implements PublishService {
    @Autowired
    JestClient jestClient;

    @Override
    public int getDauToal(String date) {
        int total = 0;
//        String Query = "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"logDate\": \"2019-04-05\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        searchSourceBuilder.query(boolQueryBuilder);
        String query = searchSourceBuilder.toString();
        System.out.println(query);
        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult res = jestClient.execute(search);
            total = res.getTotal();
            System.out.println(total);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHourToal(String date) {
        HashMap dauHourMap = new HashMap();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", date));
        sourceBuilder.query(boolQueryBuilder);
        TermsBuilder aggre = AggregationBuilders.terms("group_logHour").field("logHour").size(24);
        sourceBuilder.aggregation(aggre);
        String query = sourceBuilder.toString();
        System.out.println(query);
        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        try {
            SearchResult res = jestClient.execute(search);
            List<TermsAggregation.Entry> group_logHour = res.getAggregations().getTermsAggregation("group_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : group_logHour) {
                dauHourMap.put(bucket.getKey(), bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }
}
