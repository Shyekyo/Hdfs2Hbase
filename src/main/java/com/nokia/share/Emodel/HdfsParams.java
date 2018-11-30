package com.nokia.share.Emodel;

import com.nokia.share.hcnst.Cnst;

import java.io.Serializable;

/**
 * Created by zhangxiaofan on 2018/11/6.
 */
public class HdfsParams implements Serializable{
    private String user;
    private String fs_defaultfs;
    private String dfs_nameservice;
    private String dfs_ha_namenodes;
    private String dfs_namenode_rpc_address_ns1_nn1;
    private String dfs_namenode_rpc_address_ns1_nn2;
    private String dfs_client_failover_proxy_provider_ns1;
    private static HdfsParams hdfsParams = new HdfsParams();
    public HdfsParams() {
    }

    public static HdfsParams getDefaultParams(String user){
        hdfsParams.setUser(user);
        hdfsParams.setFs_defaultfs(Cnst.FS_DEFAULTFS);
        hdfsParams.setDfs_nameservice(Cnst.DFS_NAMESERVICE);
        hdfsParams.setDfs_ha_namenodes(Cnst.DFS_HA_NAMENODES);
        hdfsParams.setDfs_namenode_rpc_address_ns1_nn1(Cnst.DFS_NAMENODE_RPC_ADDRESS_NS1_NN1);
        hdfsParams.setDfs_namenode_rpc_address_ns1_nn2(Cnst.DFS_NAMENODE_RPC_ADDRESS_NS1_NN2);
        hdfsParams.setDfs_client_failover_proxy_provider_ns1(Cnst.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_NS1);
        return hdfsParams;
    }

    public static HdfsParams getYzParams(String user){
        hdfsParams.setUser(user);
        hdfsParams.setFs_defaultfs(Cnst.YZ_FS_DEFAULTFS);
        hdfsParams.setDfs_nameservice(Cnst.YZ_DFS_NAMESERVICE);
        hdfsParams.setDfs_ha_namenodes(Cnst.YZ_DFS_HA_NAMENODES);
        hdfsParams.setDfs_namenode_rpc_address_ns1_nn1(Cnst.YZ_DFS_NAMENODE_RPC_ADDRESS_NS1_NN1);
        hdfsParams.setDfs_namenode_rpc_address_ns1_nn2(Cnst.YZ_DFS_NAMENODE_RPC_ADDRESS_NS1_NN2);
        hdfsParams.setDfs_client_failover_proxy_provider_ns1(Cnst.YZ_DFS_CLIENT_FAILOVER_PROXY_PROVIDER_NS1);
        return hdfsParams;
    }
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFs_defaultfs() {
        return fs_defaultfs;
    }

    public void setFs_defaultfs(String fs_defaultfs) {
        this.fs_defaultfs = fs_defaultfs;
    }

    public String getDfs_nameservice() {
        return dfs_nameservice;
    }

    public void setDfs_nameservice(String dfs_nameservice) {
        this.dfs_nameservice = dfs_nameservice;
    }

    public String getDfs_ha_namenodes() {
        return dfs_ha_namenodes;
    }

    public void setDfs_ha_namenodes(String dfs_ha_namenodes) {
        this.dfs_ha_namenodes = dfs_ha_namenodes;
    }

    public String getDfs_namenode_rpc_address_ns1_nn1() {
        return dfs_namenode_rpc_address_ns1_nn1;
    }

    public void setDfs_namenode_rpc_address_ns1_nn1(String dfs_namenode_rpc_address_ns1_nn1) {
        this.dfs_namenode_rpc_address_ns1_nn1 = dfs_namenode_rpc_address_ns1_nn1;
    }

    public String getDfs_namenode_rpc_address_ns1_nn2() {
        return dfs_namenode_rpc_address_ns1_nn2;
    }

    public void setDfs_namenode_rpc_address_ns1_nn2(String dfs_namenode_rpc_address_ns1_nn2) {
        this.dfs_namenode_rpc_address_ns1_nn2 = dfs_namenode_rpc_address_ns1_nn2;
    }

    public String getDfs_client_failover_proxy_provider_ns1() {
        return dfs_client_failover_proxy_provider_ns1;
    }

    public void setDfs_client_failover_proxy_provider_ns1(String dfs_client_failover_proxy_provider_ns1) {
        this.dfs_client_failover_proxy_provider_ns1 = dfs_client_failover_proxy_provider_ns1;
    }
}
