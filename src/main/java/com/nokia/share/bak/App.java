package com.nokia.share.bak;

import com.nokia.share.hcnst.Cnst;
import com.nokia.share.common.HbaseE0984;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        FileSystem fs= null;
        String searchDir=args[0];
        String tablename=args[1];
        Path hdfsPath = new Path(Cnst.HDFS_PATH+searchDir);
        Path mvDir = new Path(Cnst.HDFS_MV_DIR);
        try {
            int count=-1;
            Configuration hdfsConf = getHdfsConf();
            fs = FileSystem.get(URI.create(Cnst.HDFS_URI), hdfsConf);
            if(fs.exists(hdfsPath)&&fs.isDirectory(hdfsPath)){
                count = mergeFile(hdfsPath, mvDir, fs);
            }
            if(count>0){
                Configuration hbaseConf = HBaseConfiguration.create();
                String tableName="";
                String cf_default="";
                HbaseE0984.createTable(hbaseConf, tableName, cf_default);
               /* try (Connection connection = ConnectionFactory.createConnection(conf)) {
                    Admin admin = connection.getAdmin();
                    //admin.createTable();
                    try (Table table = connection.getTable(TableName.valueOf(tablename))){
                        // use table as needed, the table returned is lightweight

                    }
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private static int mergeFile(Path prePath, Path resPath,FileSystem hdfs) throws Exception {
        int count = 0;
        FileStatus[] fileStatuses = hdfs.listStatus(prePath);
        FSDataOutputStream fsOutStream = hdfs.create(resPath, true);
        try {
            for (FileStatus fs : fileStatuses) {
                Path tmpPath = fs.getPath();
                Boolean isFile = fs.isDirectory() ? false : true;
                if (isFile) {
                    System.out.println(tmpPath.getName());
                    FSDataInputStream inStream = hdfs.open(tmpPath);
                    IOUtils.copyBytes(inStream, fsOutStream, 4096, false);
                    IOUtils.closeStream(inStream);
                }
                count++;
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
        } finally {
            //关闭输出流
            IOUtils.closeStream(fsOutStream);
        }
        return count;
    }
    private static Configuration getHdfsConf() {
        Configuration conf = new Configuration();
        //TODO
        return conf;
    }
}
