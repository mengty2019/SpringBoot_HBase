package com.springboot.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
@Slf4j
public class HBaseTestAPI {
    private Admin admin = null;
    private Connection connection = null;

    public HBaseTestAPI(Configuration conf) {
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            System.out.println("获取HBase连接失败!");
        }
    }
    public boolean isExistTable(String tablename){
        boolean flag=false;
        try {
            flag = admin.tableExists(TableName.valueOf(tablename));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","vm121,vm122,vm123");
        HBaseTestAPI testAPI = new HBaseTestAPI(conf);
        log.info("是否存在表："+testAPI.isExistTable("stu"));
    }
}
