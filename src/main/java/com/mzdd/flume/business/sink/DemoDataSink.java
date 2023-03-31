package com.mzdd.flume.business.sink;

import com.alibaba.fastjson.JSONObject;
import com.mzdd.flume.AbstractMysqlSink;
import com.mzdd.flume.business.entity.DemoData;

import java.util.List;

/**
 * @author mzdd
 * @create 2023-03-31 14:11
 */
public class DemoDataSink extends AbstractMysqlSink {

    public DemoDataSink() {
        this.sql = "INSERT INTO `datacloud_demo`.`demo_data`(`trace_id`,`product_id`, `product_name`) VALUES (?, ?, ?);";
    }

    @Override
    public void addLogEntity(String content, List<Object> saveLogList) {
        DemoData demo = null;
        try {
            demo = JSONObject.parseObject(content, DemoData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (demo != null) {
            saveLogList.add(demo);
        }
        List infoList = null;
//        try {
//            infoList = JSONArray.parseArray(content, DemoData.class);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        if (infoList != null) {
//            saveLogList.addAll(infoList);
//        }
    }
}
