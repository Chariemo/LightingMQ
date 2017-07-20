package com.rie.LightingMQ.producer;

import com.rie.LightingMQ.message.Message;

/**
 * Created by Charley on 2017/7/19.
 */
public interface SendHelper {

    //预发布
    Message prePublish(Message request);

    //确认发布
    void confirmPublish(Message request);

    //获取业务处理结果
    boolean getServiceStatus();
}
