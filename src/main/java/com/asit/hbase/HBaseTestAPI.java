package com.asit.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

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

    public boolean createTable(String tablename){
        boolean flag=false;
        try {
            TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(TableName.valueOf(tablename));

            ColumnFamilyDescriptor of1 = ColumnFamilyDescriptorBuilder.of("cf1");
            ColumnFamilyDescriptor of2 = ColumnFamilyDescriptorBuilder.of("cf2");

            tableDescriptorBuilder.setColumnFamily(of1);
            tableDescriptorBuilder.setColumnFamily(of2);

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);
            flag=true;
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
        String tablename="stu22";
        if(!testAPI.isExistTable(tablename)){
            testAPI.createTable(tablename);
            log.info(tablename+"表已创建成功");
        }else{
            log.info(tablename+"表已存在");
        }
    }
}
