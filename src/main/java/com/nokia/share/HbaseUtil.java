package com.nokia.share;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangxiaofan on 2018/11/26.
 */
public class HbaseUtil {
    static Configuration config = null;
    // static HTablePool pool = null;
    static {
        config = HBaseConfiguration.create();
//		config
//    .addResource(new Path(
//    		HbaseUtil.class.getResource("/hbase/hbase-site.xml").getPath()));
//		config
//    .addResource(new Path(
//    		HbaseUtil.class.getResource("/hbase/hdfs-site.xml").getPath()));
//		config
//    .addResource(new Path(
//    		HbaseUtil.class.getResource("/hbase/core-site.xml").getPath()));
//		config.set("hbase.hadoopMaster", "192.168.1.177:60000");
        config.set("hbase.zookeeper.quorum", "10.224.210.78,10.224.210.79,10.224.210.42");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("hbase.client.pause", "50");
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.rpc.timeout", "2000");
        config.set("hbase.client.operation.timeout", "3000");
        config.set("hbase.client.scanner.timeout.period", "10000");
    }
    public static final String colfinfo = "cf";
    static HConnection conn = null;

    static synchronized HConnection getHConnection() throws IOException {

        if (conn == null) {
            conn = HConnectionManager.createConnection(config);
        }
        return conn;
    }

    public static void deleteTable(String tableName) {
        System.out.println("start delete table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end delete table ......");
    }

    public static void createTable(String tableName) {
        System.out.println("start create table ......");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
            if (!hBaseAdmin.tableExists(tableName)) {
                System.out.println(tableName + " is exist,detele....");
                HTableDescriptor tableDescriptor = new HTableDescriptor(
                        tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(colfinfo));
                hBaseAdmin.createTable(tableDescriptor);
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("end create table ......");
    }


    public static void deleteRow(String tableName, String rowKey)
            throws Exception {
        HTable hTable = new HTable(config, tableName);
        Delete delete = new Delete(rowKey.getBytes());
        List<Delete> list = new ArrayList<Delete>();
        list.add(delete);
        hTable.delete(list);
    }

    public static Map<String, String> selectRowKey(String tablename,
                                                   String rowKey) throws IOException {
        Map<String, String> dataL = new HashMap<String, String>();
        HTable table = new HTable(config, tablename);
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            dataL.put(new String(kv.getQualifier()), new String(kv.getValue()));
        }
        return dataL;
    }

    public static Map<String, String> selectRowKeyFamily(String tablename,
                                                         String rowKey, String family) throws IOException {
        Map<String, String> dataL = new HashMap<String, String>();
        HTable table = new HTable(config, tablename);
        Get g = new Get(rowKey.getBytes());
        g.addFamily(Bytes.toBytes(family));
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            dataL.put(new String(kv.getQualifier()), new String(kv.getValue()));
        }
        return dataL;
    }

    public static String selectRowKeyFamilyColumn(String tablename,
                                                  String rowKey, String family, String column) throws IOException {
        HTable table = new HTable(config, tablename);
        Get g = new Get(rowKey.getBytes());
        g.addColumn(family.getBytes(), column.getBytes());
        Result rs = table.get(g);
        String value = "";
        for (KeyValue kv : rs.raw()) {
            value = new String(kv.getValue());
        }
        return value;
    }

    public static List<Map<String, String>> selectStartRowEndRowFilter(
            String tablename, String startrow, String endrow, String filterStr)
            throws IOException {
        List<Map<String, String>> dataL = new ArrayList<Map<String, String>>();
        HTable table = new HTable(config, tablename);
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(filterStr));
        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Map<String, String> data = new HashMap<String, String>();
            for (KeyValue kv : result.raw()) {
                data.put(new String(kv.getQualifier()),
                        new String(kv.getValue()));
            }
            dataL.add(data);
        }
        return dataL;
    }
    /**
     *
     * @Description:
     * 根据startkey和endkey来查询
     * @param tablename
     * @param startrow
     * @param endrow
     * @return
     * @throws IOException
     * @throws
     */
    public static List<Map<String,Object>> selectStartRowEndRow(
            String tablename, String startrow, String endrow)throws IOException {
        List<Map<String,Object>> dataL = new ArrayList<Map<String,Object>>();
        HTable table=null;;
        try {
            table = new HTable(config, tablename.trim());
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Scan scan = new Scan();
        scan.setStartRow(startrow.getBytes());
        scan.setStopRow(endrow.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        String rowKey = null;
        try {
            for (Result result : scanner) {
                Map<String,Object> d=new HashMap<String, Object>();
                for(Cell kv:result.rawCells()){
                    d.put(new String(kv.getQualifier()),
                            new String(kv.getValue()));
                }
                dataL.add(d);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataL;
    }

    //	public static void main(String[] args){
//			String uux2=  "46001313,2018070612,3421,532,2345,234,43295862,325,201807061234 ,201807061248 ,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1|138|10|10|10|10|10|1|00000000000000005ad6a644d43df67b|6|4601313|1313|8613811112222|3|43208.4156078009|43208.4156080208|11|11|11|11|11|1|0|0|0|0|0|1|0|80973|20729091|38098|6789|8765|557|2686223003|257|116|32452|1|1|1|@|2|119|10|10|10|10|10|2|00000000000000005ad6ef0df61933cd|6|4601313|1313|8613811112222|1|43208.6312675463|43208.6312684028|1|24282389|24281621|94853|94850|142|385|2416282925|257|104|1|1|2|2|2|5|1|3|3|6|1|@|3|119|10|10|10|10|10|2|00000000000000005ad6ef0df61933cd|6|4601313|1313|8613811112222|1|43208.6312675463|43208.6312684028|1|24282389|24281621|94853|94850|142|385|2416282925|257|104|147|12|2|45|7|8|9|38400|64|11|1|212|45|110|13";
//		  String column="IMSI,TIME,s_cell,s_enbid,e_cell,e_enbid,IMEI,ROAD_CELL,S_TIME,E_TIME,DURATION,RRC_CONN_STP,RRC_CONN_STP_SUC,RRC_SMC,RRC_SMC_SUC,RRC_RE_CFG,RRC_RE_CFG_SUC,RRC_RE_EST,RRC_RE_EST_SUC,RRC_REL,RRC_REL_SUC,RRC_HO_intraCELL,RRC_HO_intraCELL_SUC,RRC_HO_intraENB,RRC_HO_intraENB_SUC,RRC_HO_interENB,RRC_HO_interENB_SUC,RRC_HO_IN,RRC_HO_IN_SUC,RRC_HO_OUT,RRC_HO_OUT_SUC,RRC_PAGING_PS,RRC_PAGING_PS_SUC,RRC_PAGING_CS,RRC_PAGING_CS_SUC,CSFB Indication,REDIRECT_NETWORK,X2_handover,X2_handover_SUC,X2_handover_cancel,X2_handover_cancel_SUC,X2_SETUP,X2_SETUP_SUC,X2_RESET,X2_RESET_SUC,ENB_CONF_UPDATE,ENB_CONF_UPDATE_SUC,RES_STA_REPORT,RES_STA_REPORT_SUC,MOBILITY_SET_CH,MOBILITY_SET_CH_SUC,CELL_ACTIVE,CELL_ACTIVE_SUC,LOAD_INDICAT,LOAD_INDICAT_SUC,ERROR_INDICAT,ERROR_INDICAT_SUC,MRS,RSRP_SUM_V,RSRQ_SUM_V,period_mrs,period_RSRP,period_RSRQ,event_mrs,event_RSRP,event_RSRQ,A1_MRS,A2_MRS,A3_MRS,A4_MRS,A5_MRS,B1_MRS,B2_MRS,RSRP_1,MOD3,SINR,SINR_LOW_0,SIGNAL_ALL";
//			HbaseUtil.createTable("Test_20180711");
//		           try {
//		          	 HTable table = new HTable(HbaseUtil.config,"Test_20180711");
//		               table.setAutoFlushTo(true);//不显示设置则默认是true
//		               String rowkey  = "46001313_201807061234";
//		               Put  put = new Put(rowkey.getBytes());
//		               String[] splits= uux2.split(",");
//		               String[] columns=column.split(",");
//		               for(int i=0;i<splits.length;i++){
//		              	  put.add(HbaseUtil.colfinfo.getBytes(),columns[i].getBytes(),splits[i].getBytes());
//		               }
//		               table.put(put);
//		               table.close();//关闭hbase连接
//
//		           } catch (IOException e) {
//		               e.printStackTrace();
//		           }
//	}
//
    public static void main(String[] args) {

        try {
            List<Map<String,Object>> dataL =HbaseUtil.selectStartRowEndRow("uux2_20181126","0001", "0003");
            dataL.forEach(
                    (a -> a.forEach(
                            ((x, y) -> System.out.println("k:" + x + "v:" + y)
                            )
                    )
                    )
            );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}