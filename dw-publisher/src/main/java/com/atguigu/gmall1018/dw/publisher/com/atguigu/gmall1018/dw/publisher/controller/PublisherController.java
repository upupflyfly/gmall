package com.atguigu.gmall1018.dw.publisher.com.atguigu.gmall1018.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1018.dw.publisher.com.atguigu.gmall1018.dw.publisher.service.PublishService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {
    @Autowired
    PublishService publishService;

    @GetMapping("realtime-total")
    public String getToal(@RequestParam("date") String data) {
        List totalList = new ArrayList();
        int dauToal = publishService.getDauToal(data);

        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauToal);
        totalList.add(dauMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getRealTimeHour(@RequestParam("id") String id, @RequestParam("date") String data) {

        if ("dau".equals(id)) {
            Map dauTodayCount = publishService.getDauHourToal(data);
            String yesterDay = getYesterDay(data);
            Map dauYseterdayCount = publishService.getDauHourToal(yesterDay);
            Map<String, Object> dauMap = new HashMap<>();
            dauMap.put("today", dauTodayCount);
            dauMap.put("yesterday", dauYseterdayCount);
            return JSON.toJSONString(dauMap);
        }


        return null;
    }

    private String getYesterDay(String date) {
        Date today = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            today = df.parse(date);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterday = DateUtils.addDays(today, -1);
        String yesterdayString = df.format(yesterday);

        return yesterdayString;
    }
}
