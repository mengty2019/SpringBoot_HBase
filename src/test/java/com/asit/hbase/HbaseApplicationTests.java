package com.asit.hbase;

import com.asit.hbase.service.HBaseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@Slf4j
class HBaseApplicationTests {
    @Resource
    private HBaseService hbaseService;
    //@Resource
    //private HBaseTemplateService templateService;

    @Test
    public void getRow() {
        //templateService.getListRowkeyData("test_base",Arrays.asList("1002"),"info1","name");
    }
    //测试创建表
    @Test
    public void testCreateTable() {
        hbaseService.createTable("test_base",
                Arrays.asList(new HashMap<String,String>(){{
                    put("name","info1");
                    put("versions", "3");
                }},new HashMap<String,String>(){{
                    put("name","info2");
                    put("versions", "3");
                }},new HashMap<String,String>(){{
                    put("name","info3");
                }})
        );
    }

    //测试加入数据
    @Test
    public void testPutData() {
        hbaseService.putData("test_base", "000001", "a", new String[]{
                "project_id", "varName", "coefs", "pvalues", "tvalues",
                "create_time"}, new String[]{"40866", "mob_3", "0.9416",
                "0.0000", "12.2293", "null"});
        hbaseService.putData("test_base", "000002", "a", new String[]{
                "project_id", "varName", "coefs", "pvalues", "tvalues",
                "create_time"}, new String[]{"40866", "idno_prov", "0.9317",
                "0.0000", "9.8679", "null"});
        hbaseService.putData("test_base", "000003", "a", new String[]{
                "project_id", "varName", "coefs", "pvalues", "tvalues",
                "create_time"}, new String[]{"40866", "education", "0.8984",
                "0.0000", "25.5649", "null"});
    }

    //测试遍历全表
    @Test
    public void scanAllTable() {
        Map<String, Map<String, String>> result2 = hbaseService.queryData("stu22",null);
        log.info("-----遍历查询全表内容-----");
        result2.forEach((k, value) -> {
            log.info(k + "--->" + value);
        });
    }

    //测试遍历全表
    @Test
    public void queryData() {
        Scan scan = new Scan();
        //rowkey起始（包含）
        scan.withStartRow(Bytes.toBytes("1001"));
        //rowkey结止（包含）
        scan.withStopRow(Bytes.toBytes("1002"),true);
        Map<String, Map<String, String>> result2 = hbaseService.queryData("stu22",scan);
        log.info("-----遍历查询全表内容-----");
        result2.forEach((k, value) -> {
            log.info(k + "--->" + value);
        });
    }
}
