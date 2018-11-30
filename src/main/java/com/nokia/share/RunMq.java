package com.nokia.share;

import com.nokia.share.mq.Consumer;
import com.nokia.share.mq.Producer;
import com.nokia.share.mq.impl.MqConsumer;
import com.nokia.share.mq.impl.MqConsumerListener;
import com.nokia.share.mq.impl.MqProducer;

import javax.jms.MessageListener;

/**
 * Created by zhangxiaofan on 2018/11/19.
 */
public class RunMq {

    public static void main(String[] args) {
        String prod = null;
        String mess = null;
        String cons = null;
        if(args.length==2){
            prod = args[0];
            mess = args[1];
        }else if(args.length==1){
            cons = args[0];
        }

        Producer producer = new MqProducer(mess);
        Consumer consumer = new MqConsumer("tcp://10.224.246.91:61616", "stormDim", new MqConsumerListener());
        //new Thread((Runnable) producer,"p-1").start();
        //new Thread((Runnable) consumer,"c-1").start();
        if("p".equals(prod)){
            producer.sendMessage("stormDim");
        }else if("c".equals(cons)){
            consumer.reveiveMessage("");
        }
    }

}
