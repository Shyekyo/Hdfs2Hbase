package com.nokia.share.common;

/**
 * Created by zhangxiaofan on 2018/10/22.
 */
import com.nokia.share.hcnst.Cnst;
import com.nokia.share.face.Processer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LocalReader {

    public static void main(String args[]) throws Exception {

       /* int bufSize = 1000000;//一次读取的字节长度
        File fin = new File("C:\\Users\\SongHyeKyo\\Desktop\\小笔记.txt");//读取的文件
        //File fout = new File("D:\\test\\111111111111111111.txt");//写出的文件
        Date startDate = new Date();
        FileChannel fcin = new RandomAccessFile(fin, "r").getChannel();
        ByteBuffer rBuffer = ByteBuffer.allocate(bufSize);

        //FileChannel fcout = new RandomAccessFile(fout, "rws").getChannel();
       // ByteBuffer wBuffer = ByteBuffer.allocateDirect(bufSize);

        readFileByLine(bufSize, fcin, rBuffer, new Processer() {
            @Override
            public void doJob() {

            }
        });
        Date endDate = new Date();

        System.out.print(startDate.getTime()+"|"+endDate.getTime());*///测试执行时间
    }

    public static void readFileByLine(int bufSize, FileChannel fcin,
                                      ByteBuffer rBuffer, Configuration conf,
                                      String tableName,String cf_default,String attr,Processer processer) throws Exception {
        String enterStr = "\n";
        try {
            byte[] bs = new byte[bufSize];
            //temp：由于是按固定字节读取，在一次读取中，第一行和最后一行经常是不完整的行，因此定义此变量来存储上次的最后一行和这次的第一行的内容，
            //并将之连接成完成的一行，否则会出现汉字被拆分成2个字节，并被提前转换成字符串而乱码的问题，数组大小应大于文件中最长一行的字节数
            byte[] temp = new byte[500];
            long count=0;
            while (fcin.read(rBuffer) != -1) {
                int rSize = rBuffer.position();
                rBuffer.rewind();
                rBuffer.get(bs);
                rBuffer.clear();

                //windows下ascii值13、10是换行和回车，unix下ascii值10是换行
                //从开头顺序遍历，找到第一个换行符
                int startNum=0;
                int length=0;
                for(int i=0;i<rSize;i++){
                    if(bs[i]==10){//找到换行字符
                        startNum=i;
                        for(int k=0;k<500;k++){
                            if(temp[k]==0){//temp已经存储了上一次读取的最后一行，因此遍历找到空字符位置，继续存储此次的第一行内容，连接成完成一行
                                length=i+k;
                                for(int j=0;j<=i;j++){
                                    temp[k+j]=bs[j];
                                }
                                break;
                            }
                        }
                        break;
                    }
                }
                //将拼凑出来的完整的一行转换成字符串
                String tempString1 = new String(temp, 0, length+1, "UTF8");
                //清空temp数组
                for(int i=0;i<temp.length;i++){
                    temp[i]=0;
                }
                //从末尾倒序遍历，找到第一个换行符
                int endNum=0;
                int k = 0;
                for(int i=rSize-1;i>=0;i--){
                    if(bs[i]==10){
                        endNum=i;//记录最后一个换行符的位置
                        for(int j=i+1;j<rSize;j++){
                            temp[k++]=bs[j];//将此次读取的最后一行的不完整字节存储在temp数组，用来跟下一次读取的第一行拼接成完成一行
                            bs[j]=0;
                        }
                        break;
                    }
                }
                //去掉第一行和最后一行不完整的，将中间所有完整的行转换成字符串
                String tempString2 = new String(bs, startNum+1, endNum-startNum, "utf8");

                //拼接两个字符串
                String tempString = tempString1 + tempString2;
//              System.out.print(tempString);

                int fromIndex = 0;
                int endIndex = 0;
                while ((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1) {
                    String line = tempString.substring(fromIndex, endIndex);//按行截取字符串
                    //System.out.print(line);
                    String[] a = line.split(Cnst.SP);
                    String rowKey =a[0];
                    String data = a[1];
                    //processer.doJob(conf,tableName,cf_default,rowKey,data,attr,count);
                    fromIndex = endIndex + 1;
                    count++;
                }
            }
            System.out.println("insert count :"+count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readFileByLine(int bufSize, FileChannel fcin,
                                      ByteBuffer rBuffer, FileChannel fcout, ByteBuffer wBuffer) {
        String enterStr = "\n";
        try {
            byte[] bs = new byte[bufSize];
            //temp：由于是按固定字节读取，在一次读取中，第一行和最后一行经常是不完整的行，因此定义此变量来存储上次的最后一行和这次的第一行的内容，
            //并将之连接成完成的一行，否则会出现汉字被拆分成2个字节，并被提前转换成字符串而乱码的问题，数组大小应大于文件中最长一行的字节数
            byte[] temp = new byte[500];
            while (fcin.read(rBuffer) != -1) {
                int rSize = rBuffer.position();
                rBuffer.rewind();
                rBuffer.get(bs);
                rBuffer.clear();

                //windows下ascii值13、10是换行和回车，unix下ascii值10是换行
                //从开头顺序遍历，找到第一个换行符
                int startNum=0;
                int length=0;
                for(int i=0;i<rSize;i++){
                    if(bs[i]==10){//找到换行字符
                        startNum=i;
                        for(int k=0;k<500;k++){
                            if(temp[k]==0){//temp已经存储了上一次读取的最后一行，因此遍历找到空字符位置，继续存储此次的第一行内容，连接成完成一行
                                length=i+k;
                                for(int j=0;j<=i;j++){
                                    temp[k+j]=bs[j];
                                }
                                break;
                            }
                        }
                        break;
                    }
                }
                //将拼凑出来的完整的一行转换成字符串
                String tempString1 = new String(temp, 0, length+1, "GBK");
                //清空temp数组
                for(int i=0;i<temp.length;i++){
                    temp[i]=0;
                }
                //从末尾倒序遍历，找到第一个换行符
                int endNum=0;
                int k = 0;
                for(int i=rSize-1;i>=0;i--){
                    if(bs[i]==10){
                        endNum=i;//记录最后一个换行符的位置
                        for(int j=i+1;j<rSize;j++){
                            temp[k++]=bs[j];//将此次读取的最后一行的不完整字节存储在temp数组，用来跟下一次读取的第一行拼接成完成一行
                            bs[j]=0;
                        }
                        break;
                    }
                }
                //去掉第一行和最后一行不完整的，将中间所有完整的行转换成字符串
                String tempString2 = new String(bs, startNum+1, endNum-startNum, "GBK");

                //拼接两个字符串
                String tempString = tempString1 + tempString2;
//              System.out.print(tempString);

                int fromIndex = 0;
                int endIndex = 0;
                while ((endIndex = tempString.indexOf(enterStr, fromIndex)) != -1) {
                    String line = tempString.substring(fromIndex, endIndex)+enterStr;//按行截取字符串
                    System.out.print(line);
                    //写入文件
                    writeFileByLine(fcout, wBuffer, line);

                    fromIndex = endIndex + 1;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readFileByLine(BufferedReader bufferedReader, Configuration conf,HBaseAdmin admin,
                                       String tableName, String cf_default, String attr, Processer processer) throws Exception {
        try {
            HTable table=new HTable(conf, tableName);
            String line = null;
            long count=0;
            table.setAutoFlush(false);
            table.setWriteBufferSize(5*1024*1024);
            while((line = bufferedReader.readLine()) != null){
                String[] a = line.split(Cnst.TAB);
                String rowKey =a[0];
                String data = a[1];
                count++;
                processer.doJob(table,tableName,cf_default,rowKey,data,attr,count);
            }
            table.close();
            System.out.println("success insert count :"+count);
        } finally {
            bufferedReader.close();
        }
    }
    /**
     * 写到文件上
     * @param fcout
     * @param wBuffer
     * @param line
     */
    @SuppressWarnings("static-access")
    public static void writeFileByLine(FileChannel fcout, ByteBuffer wBuffer,
                                       String line) {
        try {
            fcout.write(wBuffer.wrap(line.getBytes("UTF-8")), fcout.size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}