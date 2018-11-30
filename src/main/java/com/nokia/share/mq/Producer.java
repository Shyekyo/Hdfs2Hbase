package com.nokia.share.mq;

/**
 * Created by zhangxiaofan on 2018/11/19.
 */
public interface Producer {
    void sendMessage(String topic);
}
