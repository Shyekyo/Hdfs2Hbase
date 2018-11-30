package com.nokia.share.face;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * Created by zhangxiaofan on 2018/10/23.
 */
public interface Processer {
    void doJob(HTable table, String tableName, String cf_default, String rowKey, String data, String att, long num) throws Exception;
}
