package com.nokia.share;

import com.nokia.share.serviceRunnable.ToHbaseService;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zhangxiaofan on 2018/12/13.
 */
public class RunTest {
    public static void main(String[] args) {
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.valueOf(10));
        for(int i=0 ; i<2000 ;i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(1);
                }
            });
        }
        pool.shutdown();
    }
}
