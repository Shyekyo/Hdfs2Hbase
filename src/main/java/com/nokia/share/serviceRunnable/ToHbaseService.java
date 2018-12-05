package com.nokia.share.serviceRunnable;

import com.nokia.share.common.HbaseE0984;
import com.nokia.share.hcnst.Cnst;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;
import java.util.stream.Stream;

/**
 * Created by zhangxiaofan on 2018/11/27.
 */
public class ToHbaseService implements Runnable{

    private FileSystem fs;
    private Configuration conf;
    private HBaseAdmin admin;
    private String tableName;
    private String cf_default;
    private String attr;
    private StackElement stack;
    private HbaseE0984 hbaseE0984 = null;

    public ToHbaseService() {
    }

    public ToHbaseService(FileSystem fs, Configuration conf, HBaseAdmin admin, String tableName, String cf_default, String attr,StackElement stack) {
        this.fs = fs;
        this.conf = conf;
        this.admin = admin;
        this.tableName = tableName;
        this.cf_default = cf_default;
        this.attr = attr;
        this.stack = stack;
        this.hbaseE0984 = new HbaseE0984();
    }

    private void toHbase(FileSystem fs, Path hdfsPath,
                         Configuration conf, HBaseAdmin admin,
                         String tableName, String cf_default, String attr) throws IOException {
        HTable table=new HTable(conf, tableName);
        long count=0;
        table.setAutoFlush(false);
        table.setWriteBufferSize(10*1024*1024);
        FSDataInputStream in = null;
        //==
        if(fs.exists(hdfsPath)){
            System.out.println("hdfsPath : "+hdfsPath);
            BufferedReader bufferedReader = null;
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(hdfsPath, false);
            while (listFiles.hasNext()) {
                LocatedFileStatus next = listFiles.next();
                Path path2 = next.getPath();
                if(path2.getName().startsWith(".")||path2.getName().endsWith("crc")){
                    continue;
                }
                in = fs.open(path2, 4096);
                System.out.println("文件名 : "+path2.getName());
                bufferedReader = new BufferedReader(new InputStreamReader(in));
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] rowKeyAnddata = line.split(Cnst.TAB);
                    String rowKey = rowKeyAnddata[0];
                    String data = rowKeyAnddata[1];
                    count++;
                    hbaseE0984.addData(table,tableName,rowKey,data,cf_default,attr,count);
                }
                in.close();
            }
        }
        //==
    }


    private void toHbaseTmp(FileSystem fs, Path hdfsPath,
                         Configuration conf, HBaseAdmin admin,
                         String tableName, String cf_default, String attr) throws Exception {
        HTable table=new HTable(conf, tableName);
        //HConnection connection = table.getConnection();
        long count=0;
        table.setAutoFlush(false);
        table.setWriteBufferSize(10*1024*1024);
        FSDataInputStream in = null;
        //==
        if(fs.exists(hdfsPath)){
            System.out.println("hdfsPath : "+hdfsPath);
            BufferedReader bufferedReader = null;
            in = fs.open(hdfsPath, 4096);
            System.out.println("文件名 : "+hdfsPath.getName());
            List<Put> list = new ArrayList<Put>();
            //bufferedReader = new BufferedReader(new InputStreamReader(in));
            long size = 0l;
            LineIterator lineIterator = IOUtils.lineIterator(in,"utf-8");
            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                String[] rowKeyAnddata = line.split(Cnst.TAB);
                String rowKey = rowKeyAnddata[0];
                String data = rowKeyAnddata[1];
                size += data.getBytes().length;
                count++;
                if(count%200==0){
                    System.out.println(Thread.currentThread().getName() +" add: "+count+" size: "+size);
                }
                hbaseE0984.addData2(table,tableName,rowKey,data,cf_default,attr,count,list);
            }
            table.put(list);
            list.clear();
            table.flushCommits();
            in.close();
        }
        //==
    }
    @Override
    public void run() {
        Throwable thrown = null;
        try {
            long startime=System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName() +" 开始时间 : "+ (startime)/1000);

            Path hdfsPath = stack.pop();
            toHbaseTmp(fs, hdfsPath, conf, admin, tableName, cf_default, attr);
            long endtime = System.currentTimeMillis();
            System.out.println(Thread.currentThread().getName() + " 执行时间 : " + (endtime - startime) / 1000);
        } catch (Throwable e) {
            thrown = e;
        }finally {
            if(thrown!=null){
                threadDeal(this,thrown);
            }
        }
    }

    public void threadDeal(Runnable r, Throwable t)
    {
        t.printStackTrace();
        System.out.println(Thread.currentThread().getName() + "==Exception==: "+ t.getMessage());
    }
}
