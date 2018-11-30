package com.nokia.share.mq.impl;

import com.nokia.share.mq.Consumer;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangxiaofan on 2018/11/19.
 */
public class MqConsumer implements Consumer,Runnable{
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;

    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;

    private static final String BROKEN_URL = ActiveMQConnection.DEFAULT_BROKER_URL;

    ConnectionFactory connectionFactory;

    Connection connection;

    Session session;

    MessageListener messageListener;

    Topic topic;

    ThreadLocal<MessageConsumer> threadLocal = new ThreadLocal<>();
    AtomicInteger count = new AtomicInteger();

    public MqConsumer() {
    }

    public MqConsumer(String url, String topicName,MessageListener messageListener) {
        this.messageListener = messageListener;
        init(url,topicName);
    }

    public void init(String url,String topicName){
        if(url==null||"".equals(url)) url=BROKEN_URL;
        try {
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,url);
            connection  = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
            topic = session.createTopic(topicName);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void reveiveMessage(String discard) {
        try {
            MessageConsumer consumer = null;

            if(threadLocal.get()!=null){
                consumer = threadLocal.get();
            }else{
                consumer = session.createConsumer(topic);
                threadLocal.set(consumer);
            }
           // while(true){
            //Thread.sleep(1000);
            //TextMessage msg = (TextMessage) consumer.receive();
            consumer.setMessageListener(messageListener);
           // }
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        reveiveMessage("");
    }
}
