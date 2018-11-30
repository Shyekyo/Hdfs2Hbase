package com.nokia.share.mq.impl;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * Created by zhangxiaofan on 2018/11/19.
 */
public class MqConsumerListener implements MessageListener{
    @Override
    public void onMessage(Message message) {
        try {
            System.out.println(Thread.currentThread().getName()+": "+((TextMessage)message).getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
