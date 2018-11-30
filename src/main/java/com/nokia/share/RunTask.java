package com.nokia.share;

import com.nokia.share.Emodel.HdfsParams;
import com.nokia.share.common.HbaseE0984;
import com.nokia.share.common.LocalReader;
import com.nokia.share.face.Processer;
import com.nokia.share.hcnst.Cnst;
import com.nokia.share.serviceRunnable.StackElement;
import com.nokia.share.serviceRunnable.ToHbaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zhangxiaofan on 2018/10/23.
 */
public class RunTask {
    public static void main(String[] args) {
        try {
            String path = args[0];
            String tableName = args[1];
            String cf_default = args[2];
            String attr = args[3];
            String zk = args[4];
            String flag = args[5];
            String delRowKey ="";
            if("deldata".equals(flag)){
                delRowKey = args[6];
            }
            /*
             flag =  putall
             flag =  put
             flag =  scan
             flag =  deldata
             flag =  droptable
             flag =  count
             */
            System.out.println("参数 : path: "+path+"   tableName: "+tableName+
                    "   cf_default: "+cf_default+"   attr: "+attr+"   zk:"+zk+"   flag:"+flag);
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum",zk);
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            config.setInt("hbase.rpc.timeout",20000);
            config.setInt("hbase.client.operation.timeout",30000);

            switch (flag) {
                case "hdfs2hbase":
                    try {
                    /*RunHdfsTask.getHdfsFileSystem(HdfsParams.getDefaultParams("weihu"));*/

                        /*ExecutorService*/ThreadPoolExecutor pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(15);
                        //hbase
                        //HbaseE0984.createTable(config,tableName,cf_default);
                        HbaseE0984.createPartitionTable(config,tableName,cf_default,
                                Bytes.toBytes("1000"),Bytes.toBytes("9000"),10);
                        HBaseAdmin admin = new HBaseAdmin(config);
                        int bufSize = 1000000;//一次读取的字节长度
                        //hbase
                        //hdfs
                        FileSystem fs = RunHdfsTask.getYzHdfsFileSystem(HdfsParams.getYzParams("nokiats"));
                        Path hdfsPath = new Path(Cnst.YZ_FS_DEFAULTFS+path);
                        StackElement stack = new StackElement();
                        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(hdfsPath, false);
                        while (listFiles.hasNext()) {
                            LocatedFileStatus next = listFiles.next();
                            Path path2 = next.getPath();
                            if (path2.getName().startsWith(".") || path2.getName().endsWith("crc")) {
                                continue;
                            }
                            stack.push(path2);
                        }
                        int size = stack.getStack().size();
                        System.out.println("文件队列大小 : "+size);
                        for(int i=0 ; i<size ;i++) {
                            pool.execute(new Thread(new ToHbaseService(fs,config,admin,tableName, cf_default, attr,stack)));
                        }
                        //toHbase(fs, hdfsPath,config,admin,tableName, cf_default, attr);
                        //hdfs
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return;
                case "puthbase":
                    try {
                    /*RunHdfsTask.getHdfsFileSystem(HdfsParams.getDefaultParams("weihu"));*/

                       // /*ExecutorService*/ThreadPoolExecutor pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);
                        //hbase
                        //HbaseE0984.createTable(config,tableName,cf_default);
                        HbaseE0984.createPartitionTable(config,tableName,cf_default,
                                Bytes.toBytes("1000"),Bytes.toBytes("9000"),10);
                        HBaseAdmin admin = new HBaseAdmin(config);
                        int bufSize = 1000000;//一次读取的字节长度
                        //hbase
                        //hdfs
                        FileSystem fs = RunHdfsTask.getYzHdfsFileSystem(HdfsParams.getYzParams("nokiats"));
                        Path hdfsPath = new Path(Cnst.YZ_FS_DEFAULTFS+path);
                        //StackElement stack = new StackElement();
                        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(hdfsPath, false);
                        while (listFiles.hasNext()) {
                            LocatedFileStatus next = listFiles.next();
                            Path path2 = next.getPath();
                            if (path2.getName().startsWith(".") || path2.getName().endsWith("crc")) {
                                continue;
                            }
                            //stack.push(path2);
                            toHbase(fs, path2,config,admin,tableName, cf_default, attr);
                        }
                        //int size = stack.getStack().size();
                        //System.out.println("文件队列大小 : "+size);
                        //while(!stack.empty()) {
                         //   pool.execute(new Thread(new ToHbaseService(fs,config,admin,tableName, cf_default, attr,stack)));
                        //}
                        //toHbase(fs, hdfsPath,config,admin,tableName, cf_default, attr);
                        //hdfs
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return;
                case "putall":
                    long startime2=System.currentTimeMillis();
                    HbaseE0984.createTable(config,tableName,cf_default);
                    HBaseAdmin admin2 = new HBaseAdmin(config);
                    int bufSize2 = 1000000;//一次读取的字节长度
                    File fin2 = new File(path);//读取的文件
                    File[] files2 = fin2.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            boolean flag = true;
                            if(name.startsWith(".")){
                                flag=false;
                            }
                            return flag;
                        }
                    });

                    long endtime2=System.currentTimeMillis();
                    System.out.println((endtime2 - startime2)/1000);
                    return;
                case "put":
                    long startime1=System.currentTimeMillis();
                    HbaseE0984.createTable(config,tableName,cf_default);
                    HBaseAdmin admin1 = new HBaseAdmin(config);
                    int bufSize1 = 1000000;//一次读取的字节长度
                    File fin1 = new File(path);//读取的文件
                    //File fin = new File("C:\\Users\\SongHyeKyo\\Desktop\\小笔记.txt");//读取的文件
                   // FileChannel fcin = new RandomAccessFile(fin, "r").getChannel();
                   // ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);
                    BufferedReader bufferedReader1 = new BufferedReader(new InputStreamReader(new FileInputStream(fin1)));
                    LocalReader.readFileByLine(bufferedReader1,config,admin1,tableName,cf_default,attr,new Processer() {
                        @Override
                        public void doJob(HTable tb, String table, String cf, String rowKey, String data, String att, long num) throws Exception {
                            HbaseE0984.addData(tb,table,rowKey,data,cf,att,num);
                        }
                    });
                    long endtime1=System.currentTimeMillis();
                    System.out.println((endtime1 - startime1)/1000);
                    return;
                case "scan":
                    HbaseE0984.getAllData(config,tableName);
                    return;
                case "deldata":
                    HbaseE0984.deleteDate(config,tableName,delRowKey);
                    return;
                case "droptable":
                    HbaseE0984.dropTable(config,tableName);
                    return;
                case "count":
                    HbaseE0984.getCount(config,tableName);
                    return;
                }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void toHbase(FileSystem fs,Path hdfsPath,
                                                          Configuration conf,HBaseAdmin admin,
                                                          String tableName, String cf_default, String attr) throws IOException {
        HTable table=new HTable(conf, tableName);
        long count=0;
        table.setAutoFlush(false);
        table.setWriteBufferSize(5*1024*1024);
        FSDataInputStream in = null;
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
                    HbaseE0984.addData(table,tableName,rowKey,data,cf_default,attr,count);
                }
                in.close();
            }
        }
    }
}
