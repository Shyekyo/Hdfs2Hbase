package com.nokia.share;

import static org.junit.Assert.assertTrue;

import com.nokia.share.serviceRunnable.ToHbaseService;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    public static void main(String[] args) {
        /*ExecutorService*/ThreadPoolExecutor pool = (ThreadPoolExecutor)Executors.newFixedThreadPool(10);
        int count = 0;
        System.out.println("active "+pool.getActiveCount());
        System.out.println("pools "+pool.getPoolSize());
        while(true) {
            //System.out.println(count++);
            count++;
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        StringBuffer stringBuffer = new StringBuffer();
                        stringBuffer.append(2);
                        //Thread.sleep(50000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            if(count<50){
                System.out.println(pool.getActiveCount()+"====");
            }
        }
    }
}
