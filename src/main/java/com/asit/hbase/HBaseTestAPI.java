package com.asit.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    /**
     * 创建表 create <table>, {NAME => <column family>, VERSIONS => <VERSIONS>}
     *
     * @param tableName
     * @param columnFamily
     * @return 是否创建成功
     */
    public boolean createTable(String tableName, List<Map<String,String>> columnFamily) {
        try {
            //列族column family
            List<ColumnFamilyDescriptor> cfDesc = new ArrayList<>(columnFamily.size());
            columnFamily.forEach(cf -> {
                ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf.get("name")));
                if(cf.containsKey("versions")){
                    cfdb.setMaxVersions(Integer.parseInt(cf.get("versions")));
                }
                cfDesc.add(cfdb.build());
            });
            //表 table
            TableDescriptor tableDesc = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(cfDesc).build();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                log.debug("table Exists!");
            } else {
                admin.createTable(tableDesc);
                log.debug("create table Success!");
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("创建表{0}失败", tableName), e);
            return false;
        } finally {
            close(admin, null, null);
        }
        return true;
    }
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        /*
        等同于上面的Configuration conf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","vm121,vm122,vm123");
        */
        HBaseTestAPI testAPI = new HBaseTestAPI(conf);
        String tablename="stu11";
        if(!testAPI.isExistTable(tablename)){
            List<Map<String,String>> list = new ArrayList<>();
            list.add(new HashMap<String,String>(){{
                put("name","info1");
                put("versions","3");
            }});
            list.add(new HashMap<String,String>(){{
                put("name","info2");
                put("versions","5");
            }});
            list.add(new HashMap<String,String>(){{
                put("name","info3");
            }});
            testAPI.createTable(tablename,list);
            log.info(tablename+"表已创建成功");
        }else{
            log.info(tablename+"表已存在");
        }
    }
    /**
     * 关闭流
     *
     * @param admin
     * @param rs
     * @param table
     */
    private void close(Admin admin, ResultScanner rs, Table table) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                log.error("关闭Admin失败", e);
            }

            if (rs != null) {
                rs.close();
            }

            if (table != null) {
                rs.close();
            }

            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("关闭Table失败", e);
                }
            }
        }
    }
}
