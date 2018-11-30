package com.nokia.share.paramLoad;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhangxiaofan on 2018/10/24.
 */
public class ParamLoader {
    private String path = null;
    public ParamLoader() {}

    public ParamLoader(String path) {
        this.path=path;
    }

    public static Map<?,?> loadProperties(){
        String file = "param.properties";
        return  loadProperties(ParamLoader.class.getClassLoader().getResource(file).getPath());
    }

    public static Map<?,?> loadProperties(String path){
        Map kvMap = new HashMap<String,String>();
        String defaultFilePath = "/usr/mining/roam/conf/universalroam.conf";
        String filePath = path;
        File confFile = new File(filePath);
        File defaultConfFile = new File(defaultFilePath);
        if (!confFile.exists()) {
            System.out.println("指定配置文件不存在，读取默认配置文件 "+defaultFilePath);
            if (defaultConfFile.exists()) filePath = defaultFilePath;
            else System.out.println("默认配置文件 "+defaultFilePath+" 不存在，使用程序默认值！");
        }
        Properties property = new Properties();
        //如果报load配置文件的错误就把下面一行注释掉
        try {
            property.load(ParamLoader.class.getClassLoader().getResourceAsStream(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kvMap;
    }
}