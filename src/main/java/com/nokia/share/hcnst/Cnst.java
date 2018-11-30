package com.nokia.share.hcnst;

/**
 * Created by zhangxiaofan on 2018/10/19.
 */
public class Cnst {
    public final static String HDFS_URI = "hdfs://bjdblcluster";
    public final static String HDFS_PATH = "";
    public final static String HDFS_MV_DIR = "/tmp/bak/";
    public final static String SP = "#";
    public final static String TAB = "\t";
    //storm维表
    public final static String FS_DEFAULTFS = "hdfs://bjdblcluster";
    public final static String DFS_NAMESERVICE = "bjdblcluster";
    public final static String DFS_HA_NAMENODES = "nn1,nn2";
    public final static String DFS_NAMENODE_RPC_ADDRESS_NS1_NN1 = "10.224.246.5:8020";
    public final static String DFS_NAMENODE_RPC_ADDRESS_NS1_NN2 = "10.224.246.4:8020";
    public final static String DFS_CLIENT_FAILOVER_PROXY_PROVIDER_NS1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

    //亦庄
    public final static String YZ_FS_DEFAULTFS = "hdfs://bjdpi2cluster";
    public final static String YZ_DFS_NAMESERVICE = "bjdpi2cluster";
    public final static String YZ_DFS_HA_NAMENODES = "nn1,nn2";
    public final static String YZ_DFS_NAMENODE_RPC_ADDRESS_NS1_NN1 = "10.224.216.4:8020";
    public final static String YZ_DFS_NAMENODE_RPC_ADDRESS_NS1_NN2 = "10.224.216.5:8020";
    public final static String YZ_DFS_CLIENT_FAILOVER_PROXY_PROVIDER_NS1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";


}
