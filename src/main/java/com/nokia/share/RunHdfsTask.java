package com.nokia.share;

import com.nokia.share.DimModel.*;
import com.nokia.share.hcnst.Cnst;
import com.nokia.share.Emodel.HdfsParams;
import com.nokia.share.face.DimensionFace;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.*;

/**
 * Created by zhangxiaofan on 2018/11/6.
 */
public class RunHdfsTask {
    //private static final Logger logger = LoggerFactory.getLogger(RunHdfsTask.class);
    public static void main(String[] args) {
        hdfsDim();
    }
    public static void hdfsDim(){
        zyjdApp();
        allCellInfo();
        companyIPInfo();
        doCountry();
        allMmeInfo();
        mscBscName();
        doProvince();
        sceneLv3();
        sgsName();
        terminalInfo();
        workingBandDaoInfo();
        topapp();
        mobikeUser();
        securitySceneInfo();
        allSgwInfo();
    }

    public static Map<String,String> topapp() {
        String dimensionDir="/stormDimension/topapp/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    map.put(split[0]+"_"+split[1],split[2]);
//                    String app_type = sqlRowSet.getString(1);
//                    String app_sub_type = sqlRowSet.getString(2);
//                    String top_app = sqlRowSet.getString(3);
//                    map.put(app_type+"_"+app_sub_type,top_app);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }

    public static Map<String,String> zyjdApp() {
        String dimensionDir="/stormDimension/zyjdapp/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    map.put(split[0]+"_"+split[1],split[2]);
//                    String app_type = sqlRowSet.getString(1);
//                    String app_sub_type = sqlRowSet.getString(2);
//                    String if_show = sqlRowSet.getString(3);
//                    map.put(app_type+"_"+app_sub_type,if_show);

                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }


    public static Map<String,CellInfo> allCellInfo() {
        String dimensionDir="/stormDimension/allCellInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension2(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
//                    String cell_id = split[0];
//                    String town_oid = split[1];
//                    map.put(cell_id, town_oid);
                    CellInfo info = new CellInfo();
                    info.setCellId(split[0]);
                    info.setEci(split[1]);
                    info.setCellName(split[2]);
                    info.setCellOid(split[3]);
                    info.setSubRegionOid(split[4]);
                    info.setAreaId(split[5]);
                    info.setNeType(!"".equals(split[6])?Integer.valueOf(split[6]):-1);
                    info.setMscOid(split[7]);
                    info.setTown_oid(split[8]);
                    if(info.getNeType()!=4&&info.getCellId()!=null){
                        String[] ss = info.getCellId().split("_");
                        if(ss.length==2){
                            info.setCell_sac(ss[0]);
                            info.setLac(ss[1]);
                        }
                    }
                    map.put(info.getCellId(),info);


                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, CellInfo>) map;
    }

    public static Map<String,CompanyIPInfo> companyIPInfo() {
        String dimensionDir="/stormDimension/companyIPInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map = null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    CompanyIPInfo info = new CompanyIPInfo();
                    String[] split = line.split("\\|");
                    info.setAppServerIp(split[0]);
                    info.setAppType(split[1]);
                    info.setAppSubType(split[2]);
                    info.setCompany(split[3]);
                    map.put(info.getAppServerIp(),info);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, CompanyIPInfo>) map;
    }

    public static Map<Integer,Map<String,String>> doCountry() {
        String dimensionDir="/stormDimension/country/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    int codelength = Integer.valueOf(split[0]);
                    String code = split[1];
                    String name = split[2];
                    Map<String,String> inmap = (Map<String,String>)map.get(codelength);
                    if(inmap==null){
                        inmap = new HashMap<String,String>();
                        map.put(codelength, inmap);
                    }
                    inmap.put(code,name);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<Integer, Map<String, String>>) map;
    }

    public static Map<String,String> allMmeInfo() {
        String dimensionDir="/stormDimension/allMmeInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    String mme_ip_str = split[0];
                    String mme_oid = split[1];
                    map.put(mme_ip_str, mme_oid);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }

    public static Map<Long,Boolean> mobikeUser() {
        String dimensionDir="/stormDimension/mobikeUser/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    Long imsi = Long.valueOf(split[0]);
                    map.put(imsi,true);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<Long, Boolean>) map;
    }

    public static Map<String,String> mscBscName() {
        String dimensionDir="/stormDimension/mscBscName/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    String tnesidAndSms_bsc_id = split[0];
                    String msc_idAndBsc_name= split[1];
                    map.put(tnesidAndSms_bsc_id,msc_idAndBsc_name);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }

    public static Map<Integer,Map<String,String>> doProvince() {
        String dimensionDir="/stormDimension/province/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    int codelength = Integer.valueOf(split[0]);
                    String code = split[1];
                    String name = split[2];
                    Map<String,String> inmap = (Map<String,String>)map.get(codelength);
                    if(inmap==null){
                        inmap = new HashMap<String,String>();
                        map.put(codelength, inmap);
                    }
                    inmap.put(code,name);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<Integer, Map<String, String>>) map;
    }

    public static Map<String,Map<String,SecuritySceneInfo>> securitySceneInfo() {
        String dimensionDir="/stormDimension/securitySceneInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    SecuritySceneInfo info = new SecuritySceneInfo();
                    info.setSecurityScene(split[0]);
                    info.setSecuritySceneLv1(split[1]);
                    info.setSecuritySceneLv2(split[2]);
                    info.setSecuritySceneLv3(split[3]);
                    info.setOwnName(split[4]);
                    info.setStatus(Integer.valueOf(split[5]));
                    Map<String,SecuritySceneInfo> inner = (Map<String, SecuritySceneInfo>) map.get(info.getSecurityScene());
                    if(inner==null){
                        inner = new HashMap<>();
                        map.put(info.getSecurityScene(),inner);
                    }
                    inner.put(info.getSecuritySceneLv3(),info);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, Map<String, SecuritySceneInfo>>) map;
    }

    public static Map<String,List<String>> sceneLv3() {
        String dimensionDir="/stormDimension/sceneLv3/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension2(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    String cellId = split[0];
                    String securitySceneLv3 = split[1];
                    List<String> list = (List<String>)map.get(cellId);
                    if(list==null){
                        list = new ArrayList<String>();
                        map.put(cellId,list);
                    }
                    list.add(securitySceneLv3);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, List<String>>) map;
    }

    public static Map<String,String> sgsName() {
        String dimensionDir="/stormDimension/sgsName/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    String ip = split[0];
                    String name = split[1];
                    map.put(ip, name);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }

    public static Map<String,SgwInfo> allSgwInfo() {
        String dimensionDir="/stormDimension/allSgwInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
//                    String ip = split[0];
//                    String name = split[1];
//                    map.put(ip, name);
                    SgwInfo info = new SgwInfo();
                    info.setIp(split[0]);
                    info.setName(split[1]);
                    info.setOid(split[2]);
                    map.put(info.getIp(),info);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, SgwInfo>) map;
    }

    public static void terminalInfo() {
        String dimensionDir="/stormDimension/terminalInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        try {
            Map<?,?> map = getDimension2(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    TerminalInfo info = new TerminalInfo();
                    info.setCode(split[0]);
                    info.setBrandName(split[1]);
                    info.setName(split[2]);
                    //logger.info("加载terminal:"+info);
                    map.put(info.getCode(),info);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String,String> workingBandDaoInfo() {
        String dimensionDir="/stormDimension/workingBandDaoInfo/";
        Path hdfsPath = new Path(Cnst.FS_DEFAULTFS+dimensionDir);
        Map<?,?> map =null;
        try {
            map = getDimension(HdfsParams.getDefaultParams("weihu"), hdfsPath, new DimensionFace() {
                @Override
                public void doJob(Map map, String line) throws Exception {
                    String[] split = line.split("\\|");
                    String ecelEciCode = split[0];
                    String workingBand = split[1];
                    map.put(ecelEciCode,workingBand);
                }
            });
            System.out.println(dimensionDir+" => size : "+map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (Map<String, String>) map;
    }

    /**
     *
     * @param hdfsParams 连接hdfs需要参数
     * @return
     */
    public static FileSystem getHdfsFileSystem(HdfsParams hdfsParams){
        FileSystem fs = null;
        Configuration hdfsConf = getHdfsConf(hdfsParams);
        try {
            fs = FileSystem.get(new URI(Cnst.FS_DEFAULTFS), hdfsConf, hdfsParams.getUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }


    /**
     *
     * @param hdfsParams 连接hdfs需要参数
     * @return
     */
    public static FileSystem getYzHdfsFileSystem(HdfsParams hdfsParams){
        FileSystem fs = null;
        Configuration hdfsConf = getHdfsConf(hdfsParams);
        try {
            fs = FileSystem.get(new URI(Cnst.YZ_FS_DEFAULTFS), hdfsConf, hdfsParams.getUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }


    /**
     *
     * @param hdfsParams 连接hdfs需要参数
     * @return
     */
    private static Configuration getHdfsConf(HdfsParams hdfsParams) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsParams.getFs_defaultfs());
        conf.set("dfs.nameservices", hdfsParams.getDfs_nameservice());
        conf.set("dfs.ha.namenodes."+ hdfsParams.getDfs_nameservice(), hdfsParams.getDfs_ha_namenodes());
        conf.set("dfs.namenode.rpc-address."+ hdfsParams.getDfs_nameservice()+".nn1", hdfsParams.getDfs_namenode_rpc_address_ns1_nn1());
        conf.set("dfs.namenode.rpc-address."+ hdfsParams.getDfs_nameservice()+".nn2", hdfsParams.getDfs_namenode_rpc_address_ns1_nn2());
        conf.set("dfs.client.failover.proxy.provider."+ hdfsParams.getDfs_nameservice(), hdfsParams.getDfs_client_failover_proxy_provider_ns1());
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    /**
     *
     * @param hdfsParams 连接hdfs需要参数
     * @param hdfsPath   hdfs数据所在目录
     * @param df         自定义实现数据加载方式
     * @return
     * @throws Exception
     */
    public static Map<?,?> getDimension(HdfsParams hdfsParams,Path hdfsPath,DimensionFace df) throws Exception {
        FileSystem fs = getHdfsFileSystem(hdfsParams);
        boolean exists = fs.exists(hdfsPath);
        System.out.println("dir is exists : "+exists);
        Map<?,?> map = null;
        if(exists) {
            map = new HashMap<>();
            FSDataInputStream in = null;
            //BufferedReader bufferedReader = null;
            LineIterator lineIterator =null;
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(hdfsPath, false);
            while (listFiles.hasNext()) {
                LocatedFileStatus next = listFiles.next();
                Path path = next.getPath();
                in = fs.open(path, 4096);
            }
            //InputStream in = new URL(hdfsPath.toString()).openStream();
            //IOUtils.copyBytes(in,System.out,4096,false);
            try {
                //bufferedReader = new BufferedReader(new InputStreamReader(in));
                lineIterator = org.apache.commons.io.IOUtils.lineIterator(in, "utf-8");
                while (lineIterator.hasNext()) {
                    String line = lineIterator.next();
                    //                String[] kv = line.split("\\|");
    //                map.put(kv[0], kv[1]);
                    df.doJob(map,line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lineIterator.close();
                in.close();
            }
            //map.forEach((k,v) -> System.out.println(k +":"+v));
            //logger.info("dimensionDir : " + map.size());
        }
        return map;
    }

    public static Map<?,?> getDimension2(HdfsParams hdfsParams,Path hdfsPath,DimensionFace df) throws Exception {
        FileSystem fs = getHdfsFileSystem(hdfsParams);
        boolean exists = fs.exists(hdfsPath);
        System.out.println("dir is exists : "+exists);
        Map<?,?> map = null;
        if(exists) {
            map = new HashMap<>();
            FSDataInputStream in = null;
            //BufferedReader bufferedReader = null;
            LineIterator lineIterator =null;
            FileStatus[] fileStatuses = fs.listStatus(hdfsPath);
            for (FileStatus fstatus: fileStatuses) {
                //LocatedFileStatus next = listFiles.next();
                if(fstatus.isDirectory()){
                    Path path = fstatus.getPath();
                    FileStatus[] fileStatuses1 = fs.listStatus(path);
                    for(FileStatus fstatus1: fileStatuses1){
                        if(fstatus1.isFile()){
                            Path path1 = fstatus1.getPath();
                            in = fs.open(path1, 4096);
                            try {
                                //bufferedReader = new BufferedReader(new InputStreamReader(in));
                                lineIterator = IOUtils.lineIterator(in, "utf-8");
                                while (lineIterator.hasNext()) {
                                    String line = lineIterator.next();
                                    //                String[] kv = line.split("\\|");
                                    //                map.put(kv[0], kv[1]);
                                    df.doJob(map,line);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                lineIterator.close();
                                in.close();
                            }
                        }
                    }
                }else{
                    Path path2 = fstatus.getPath();
                    in = fs.open(path2, 4096);
                    try {
                        //bufferedReader = new BufferedReader(new InputStreamReader(in));
                        lineIterator = IOUtils.lineIterator(in, "utf-8");
                        while (lineIterator.hasNext()) {
                            String line = lineIterator.next();
                            //                String[] kv = line.split("\\|");
                            //                map.put(kv[0], kv[1]);
                            df.doJob(map,line);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lineIterator.close();
                        in.close();
                    }
                }
            }
            //InputStream in = new URL(hdfsPath.toString()).openStream();
            //IOUtils.copyBytes(in,System.out,4096,false);
            //map.forEach((k,v) -> System.out.println(k +":"+v));
            //logger.info("dimensionDir : " + map.size());
        }
        return map;
    }
}
