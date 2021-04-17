package com.asit.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
            this.connection = ConnectionFactory.createConnection(conf);
            admin = this.connection.getAdmin();
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
                log.info(tableName+"表已存在");
            } else {
                admin.createTable(tableDesc);
                log.info(tableName+"表已创建成功");
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("创建表{0}失败", tableName), e);
            return false;
        } finally {
            close(admin, null, null);
        }
        return true;
    }
    public void dropTable(String tablename){
        if(isExistTable(tablename)) {
            try {
                admin.disableTable(TableName.valueOf(tablename));
                admin.deleteTable(TableName.valueOf(tablename));
                log.info(tablename+"已删除");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            log.info(tablename+"表不存在");
        }
    }
    public void createNamespace(String ns){
        try {
            NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();
            admin.createNamespace(build);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putData(String tablename, String rowkey, String cf, String[] cns, String[] vals) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Put put = new Put(Bytes.toBytes(rowkey));
            final int index = 0;
            for (int i = 0; i < cns.length; i++) {
                put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cns[i]), Bytes.toBytes(vals[i]));
            }
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(admin,null,table);
        }
    }
    public void getData(String tablename,String rowkey,String cf,String cn){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            Get get = new Get(Bytes.toBytes(rowkey));
            if(cf!=null&&!"".equals(cf)){
                if(cn!=null&&!"".equals(cn)){
                    get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
                }else{
                    get.addFamily(Bytes.toBytes(cf));
                }
            }
            //设置取几个版本的数据
            get.setMaxVersions(3);
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                log.info("rowkey："+Bytes.toString(CellUtil.cloneRow(cell))+
                        "，列族："+Bytes.toString(CellUtil.cloneFamily(cell))+
                "，列名："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                "，值："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
            /*
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(cf), Bytes.toBytes(cn));
            log.info(Bytes.toString(CellUtil.cloneFamily(cell)));
            log.info(Bytes.toString(CellUtil.cloneValue(cell)));
            */
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(admin,null,table);
        }
    }

    /**
     * 通过表名及过滤条件查询数据
     *
     * @param tableName
     * @param scan
     * @return
     */
    public Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
        if(scan==null){
            scan = new Scan();
        }
        // <rowKey,对应的行数据>
        Map<String, Map<String, String>> result = new HashMap<>();
        ResultScanner rs = null;
        //获取表
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
            for (Result r : rs) {
                // 每一行数据
                Map<String, String> columnMap = new HashMap<>();
                String rowKey = null;
                // 行键，列族和列限定符一起确定一个单元（Cell）
                for (Cell cell : r.listCells()) {
                    if (rowKey == null) {
                        rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                        //rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    }
                    columnMap.put(
                            //列族+列名
                            Bytes.toString(CellUtil.cloneFamily(cell))+":"+Bytes.toString(CellUtil.cloneQualifier(cell)),
                            //Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            //值
                            Bytes.toString(CellUtil.cloneValue(cell))
                            //Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                    );
                }
                if (rowKey != null) {
                    result.put(rowKey, columnMap);
                }
            }
        } catch (IOException e) {
            log.error(MessageFormat.format("遍历查询指定表中的所有数据失败,tableName:{0}", tableName), e);
        } finally {
            close(null, rs, table);
        }
        return result;
    }
    public static void main(String[] args) {
        /**
         * public static Configuration addHbaseResources(Configuration conf) {
         *         conf.addResource("hbase-default.xml");
         *         conf.addResource("hbase-site.xml");
         *         checkDefaultsVersion(conf);
         *         return conf;
         *     }
         */
        Configuration conf = HBaseConfiguration.create();
        /*
        等同于上面的Configuration conf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","vm121,vm122,vm123");
        */
        HBaseTestAPI testAPI = new HBaseTestAPI(conf);
        /*
        String tablename="stu11";
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
        */
        testAPI.dropTable("stu11");
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
