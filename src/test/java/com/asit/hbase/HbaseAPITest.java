package com.asit.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

@Slf4j
public class HbaseAPITest {
    private HBaseTestAPI testAPI=null;
    @Before
    public void init(){
        log.info("----------初始化----------");
        Configuration conf = HBaseConfiguration.create();
        testAPI = new HBaseTestAPI(conf);
    }
    @Test
    public void createNamespace(){
        testAPI.createNamespace("testns");
    }
    @Test
    public void putData(){
        testAPI.putData("stu22","1003","info2",new String[]{"name","age"},new String[]{"lisi","120"});
    }
    @Test
    public void getData(){
        testAPI.getData("stu22","1001","info1","");
    }
    //测试遍历全表
    @Test
    public void queryData() {
        Scan scan = new Scan();
        //rowkey起始（包含）
        scan.withStartRow(Bytes.toBytes("1001"));
        //rowkey结止（包含）
        scan.withStopRow(Bytes.toBytes("1002"),true);
        Map<String, Map<String, String>> result2 = testAPI.queryData("stu22",scan);
        log.info("-----遍历查询全表内容-----");
        result2.forEach((k, value) -> {
            log.info(k + "--->" + value);
        });
    }
    @After
    public  void over(){
      log.info("----------结束----------");
    }
}
