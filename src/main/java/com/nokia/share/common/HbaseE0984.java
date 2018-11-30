package com.nokia.share.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * Created by zhangxiaofan on 2018/10/22.
 */
public class HbaseE0984 {
    public static void main(String[] args) {
        Configuration configuration=HBaseConfiguration.create();
        String tableName="student";
        String cf="cf";
        createTable(configuration, tableName,cf);
    }

    /**
     * create a new Table
     * @param configuration Configuration
     * @param tableName String,the new Table's name
     * */
    public static void createTable(Configuration configuration,String tableName,String cf){
        HBaseAdmin admin;
        try {
            admin = new HBaseAdmin(configuration);
            /*if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName+"is exist ,delete ......");
            }*/
            if(admin.tableExists(tableName)) return;

            HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
            //tableDescriptor.
            //tableDescriptor.addFamily(new HColumnDescriptor("address"));
            admin.createTable(tableDescriptor);
            System.out.println("end create table");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createPartitionTable(Configuration configuration,String tableName,String cf,byte[] startKey, byte[] endKey, int numRegions){
        HBaseAdmin admin;
        try {
            admin = new HBaseAdmin(configuration);
            /*if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName+"is exist ,delete ......");
            }*/
            if(admin.tableExists(tableName)) return;

            HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addFamily(new HColumnDescriptor(cf));
            //tableDescriptor.
            //tableDescriptor.addFamily(new HColumnDescriptor("address"));
            admin.createTable(tableDescriptor,startKey,endKey,numRegions);
            System.out.println("end create paritition table");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Delete the existing table
     * @param configuration Configuration
     * @param tableName String,Table's name
     * */
    public static void dropTable(Configuration configuration,String tableName){
        HBaseAdmin admin;
        try {
            admin = new HBaseAdmin(configuration);
            if(admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName+"delete success!");
            }else{
                System.out.println(tableName+"Table does not exist!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addData(HTable table,String tableNmae,String rowKey, String data, String cf, String attr,long num) {
       // HBaseAdmin admin;
        long count = num;
        try {
            //admin = new HBaseAdmin(config);
           // if(admin.tableExists(tableNmae)){
            Put put=new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(cf), Bytes.toBytes(attr), Bytes.toBytes(data));
            table.put(put);
//            if(count>=6000){
//                System.out.println(Thread.currentThread().getName() +"已触发手动提交: "+count);
//                table.flushCommits();
//                count=0;
//            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                table.flushCommits();
                table.close();
            } catch (Exception e1) {
                e1.printStackTrace();
                System.out.println(e1);
            }
            System.out.println("插入异常: "+num);
        }
    }

    public void addData2(HTable table,String tableNmae,String rowKey, String data, String cf, String attr,long num,List list) {
        // HBaseAdmin admin;
        long count = num;
        try {
            //admin = new HBaseAdmin(config);
            // if(admin.tableExists(tableNmae)){
            Put put=new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(cf), Bytes.toBytes(attr), Bytes.toBytes(data));
            put.setWriteToWAL(false);
            list.add(put);
            if(num%1000==0){
                table.put(put);
                list.clear();
                table.flushCommits();
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                table.flushCommits();
                table.close();
            } catch (Exception e1) {
                e1.printStackTrace();
                System.out.println(e1);
            }
            System.out.println("插入异常: "+num);
        }
    }


    public static void deleteDate(Configuration configuration,String tableName,String rowKey){
        HBaseAdmin admin;
        try {
            admin=new HBaseAdmin(configuration);
            if(admin.tableExists(tableName)){
                HTable table=new HTable(configuration, tableName);
                Delete delete=new Delete(Bytes.toBytes(rowKey));
                table.delete(delete);
                System.out.println("delete success!");
            }else{
                System.out.println("Table does not exist!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void getData(Configuration configuration,String tableName,String rowKey){
        HTable table;
        try {
            table = new HTable(configuration, tableName);
            Get get=new Get(Bytes.toBytes(rowKey));
            Result result=table.get(get);

            for(Cell cell:result.rawCells()){
                System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
                System.out.println("Timetamp:"+cell.getTimestamp()+" ");
                System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
                System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
                System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getCount(Configuration configuration,String tableName) {
        long bef = System.currentTimeMillis();
        int i = 0;
        ResultScanner rs = null;
        try {
            HTable table = new HTable(configuration,tableName);
            table.setScannerCaching(500);
            Scan s = new Scan();
            s.setCaching(500);
            s.setCacheBlocks(false);
            s.setFilter(new FirstKeyOnlyFilter());
            rs = table.getScanner(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (org.apache.hadoop.hbase.client.Result r : rs) {
            i++ ;
        }
        long now = System.currentTimeMillis();
        System.out.println("keyword表中数据总数 ：" + i + ", 所用时间 ： " + (now - bef)/1000.0);
        rs.close();
    }

    public static void getAllData(Configuration configuration,String tableName){
        HTable table;
        try {
            table=new HTable(configuration, tableName);
            Scan scan=new Scan();
            ResultScanner results=table.getScanner(scan);
            for(Result result:results){
                for(Cell cell:result.rawCells()){
                    System.out.println("rowkey: "+new String(CellUtil.cloneRow(cell))+" timetamp: "+cell.getTimestamp()+
                            " Family: "+new String(CellUtil.cloneFamily(cell))+" attr: "+new String(CellUtil.cloneQualifier(cell))+
                            " value: "+new String(CellUtil.cloneValue(cell)));
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
