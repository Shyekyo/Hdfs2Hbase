package com.nokia.share.mq.impl;

import com.nokia.share.mq.Producer;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhangxiaofan on 2018/11/19.
 */
public class MqProducer implements Producer,Runnable{
    //ActiveMq 的默认用户名
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    //ActiveMq 的默认登录密码
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    //ActiveMQ 的链接地址
    private static final String BROKEN_URL = ActiveMQConnection.DEFAULT_BROKER_URL;

    AtomicInteger count = new AtomicInteger(0);
    //链接工厂
    ConnectionFactory connectionFactory;
    //链接对象
    Connection connection;
    //事务管理
    Session session;

    String message;
    ThreadLocal<MessageProducer> threadLocal = new ThreadLocal<>();

    public MqProducer() {
        init();
    }
    public MqProducer(String msg) {
        this.message = msg;
        init();
    }

    public void init(){
        try {
            //创建一个链接工厂
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,"tcp://10.224.246.91:61616");
            //从工厂中创建一个链接
            connection  = connectionFactory.createConnection();
            //开启链接
            connection.start();
            //创建一个事务（这里通过参数可以设置事务的级别）
            session = connection.createSession(true,Session.SESSION_TRANSACTED);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void sendMessage(String topic) {
        try {
            //创建一个消息队列
            Topic tpc = session.createTopic(topic);
            //消息生产者
            MessageProducer messageProducer = null;
            if(threadLocal.get()!=null){
                messageProducer = threadLocal.get();
            }else{
                messageProducer = session.createProducer(tpc);
                threadLocal.set(messageProducer);
            }
//            while(true){
//                Thread.sleep(10000);
//                int num = count.getAndIncrement();
//                //创建一条消息
//                TextMessage msg = session.createTextMessage(Thread.currentThread().getName()+
//                        "加载维度,count:"+num);
//                /*System.out.println(Thread.currentThread().getName()+
//                        "加载维度,count:"+num);*/
//                //发送消息
//                messageProducer.send(msg);
//                //提交事务
//                session.commit();
//            }
            TextMessage msg = session.createTextMessage(message);
                /*System.out.println(Thread.currentThread().getName()+
                        "加载维度,count:"+num);*/
            //发送消息
            messageProducer.send(msg);
            //提交事务
            session.commit();
            System.out.println("send ok");
        } catch (JMSException e) {
            e.printStackTrace();
        }finally {
            try {
                connection.close();
                System.out.println("连接关闭");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        sendMessage("dim");
    }
}
