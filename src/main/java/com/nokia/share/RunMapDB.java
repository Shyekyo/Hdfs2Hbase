package com.nokia.share;

import com.nokia.share.DimModel.SgwInfo;
import com.nokia.share.DimModel.TerminalInfo;
import com.nokia.share.Emodel.HdfsParams;
import com.nokia.share.hcnst.Cnst;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mapdb.*;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by zhangxiaofan on 2018/11/13.
 */
public class RunMapDB {

    public static void main(String[] args) {
        //File dbFile = new File("D:\\78abf1b0-6852-40ad-99c8-aeb0bc08c772.map.p");
        File dbFile = new File("D:\\78abf1b0-6852-40ad-99c8-aeb0bc08c772.map");
        //DB db = DBMaker.newFileDB(dbFile).closeOnJvmShutdown().make();
        DB db = DBMaker.fileDB(dbFile).make();
        Object dbMap = db.get("78abf1b0-6852-40ad-99c8-aeb0bc08c772");
        // BTreeMap dbMap = db.getTreeMap("a");
        Iterator iterator = ((HTreeMap)dbMap).entrySet().iterator();
        System.out.println(((HTreeMap)dbMap).sizeLong());
        while (iterator.hasNext()){
            System.out.println("key:"+iterator.next().toString().replace("=ArraySeq","  value:"));
        }
    }

    public static void bak(String[] args) {
        Map<String, SgwInfo> m = new HashMap<String, SgwInfo>(){{
           put("a",new SgwInfo("sd","sd","sd"));
           put("b",new SgwInfo("we","we","We"));
           put("c",new SgwInfo("xc","xc","xc"));
        }};
        Map<String, TerminalInfo> m1 = new HashMap<String, TerminalInfo>(){{
            put("a",new TerminalInfo("Rt","rt","rt"));
            put("b",new TerminalInfo("fg","fg","fg"));
            put("c",new TerminalInfo("vb","vb","vb"));
        }};
        //DB db = DBMaker.memoryDB().closeOnJvmShutdown().make();

        DB db = DBMaker.fileDB("D:\\aaa.db").fileMmapEnableIfSupported().make();
        HTreeMap<String, SgwInfo> map =
                db.hashMap("sgw", Serializer.STRING, Serializer.JAVA).
                        createOrOpen();
        map.putAll(m);
        HTreeMap<String, TerminalInfo> map1 =
                db.hashMap("ter", Serializer.STRING, Serializer.JAVA).
                        createOrOpen();
        map1.putAll(m1);
        SgwInfo a = map.get("a");
        System.out.println(a);
        TerminalInfo a1 = map1.get("a");
        System.out.println(a1);
        System.out.println(db.exists("sgw"));
        Map sgw = (Map) db.get("sgw");
        System.out.println(sgw.get("a"));

        db.close();
    }

    private static Path getHdfsPath() {
        String dimensionDir="/mapdb/terminal/terminal.db";
        FileSystem fs = RunHdfsTask.getHdfsFileSystem(HdfsParams.getDefaultParams("weihu"));
        return new Path(Cnst.FS_DEFAULTFS+dimensionDir);
    }


}
