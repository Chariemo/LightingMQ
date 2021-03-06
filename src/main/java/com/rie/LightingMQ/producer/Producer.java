package com.rie.LightingMQ.producer;

import com.rie.LightingMQ.broker.RequestHandlerType;
import com.rie.LightingMQ.config.ConnectionConfig;
import com.rie.LightingMQ.connection.Client;
import com.rie.LightingMQ.connection.RequestFuture;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.message.TransferType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by Charley on 2017/7/19.
 */
public class Producer {

    private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private static Client client;
    private volatile Service service; //服务
    private volatile Object[] objects; //服务入口参数
    private static ConnectionConfig config;
    private static final Producer PRODUCER = new Producer();    //单例模式

    private Producer() {

    }

    public static Producer newProducer(ConnectionConfig config) {

        Producer.config = config;
        client = Client.newClientInstance(config);
        return PRODUCER;
    }

    public static Producer newProducer() {

        return newProducer(ConnectionConfig.getDefaultConConfig());

    }

    public static Producer newProducer(String configpath) {

        return newProducer(new ConnectionConfig(configpath));
    }

    public static Producer newProduce(Properties properties) {

        return newProducer(new ConnectionConfig(properties));
    }


    public boolean unsafePublish(Topic topic) {

        ArrayList<Topic> list = new ArrayList<>(1);
        list.add(topic);
        return unsafePublish(list);
    }

    public boolean unsafePublish(List<Topic> topics) { //直接发布 不关注消息是否发布成功

        boolean result = false;
        Message request = Message.newRequestMessage();
        request.setReqHandlerType(RequestHandlerType.PUBLISH.value);
        request.setBody(topics);
        result = send(request);

        return result;
    }

    public boolean send(Message request) {

        boolean result = false;
        Message response = null;
        int reSendCounter = 0;
        int reConCounter = 0;

        while (client.reConnect() && reSendCounter < config.getReSendTimes()) { //重连 、 重发
            RequestFuture responseFuture = client.write(request);
            try {
                response = responseFuture.waitResponse(config.getResponseTimeOut(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while waiting for response <- {}.", request);
                throw new RuntimeException(e.getMessage(), e);
            }

            if (response == null) {
                LOGGER.warn("timeout while waiting for response <- {}.", request);
                reSendCounter++;
            }
            else if (response.getType() == TransferType.EXCEPTION.value) {
                LOGGER.error("something wrong happened to server handling request {}.", request);
                break;
            }
            else {
                result = true;
                break;
            }
        }
        return result;
    }

    public boolean safePublish(long timeout, TimeUnit timeUnit, Topic topic) {

        ArrayList<Topic> list = new ArrayList<>(1);
        list.add(topic);
        return safePublish(timeout, timeUnit, list);
    }

    //同步发布 关注是否发布成功
    public boolean safePublish(long timeout, TimeUnit timeUnit, List<Topic> topics) {

        boolean result = false;
        Message request = Message.newRequestMessage();
        request.setReqHandlerType(RequestHandlerType.PRE_PUBLISH.value);
        request.setId(getMessageId(request.getSeqId()));
        request.setBody(topics);

        if (send(request)) {  //预发布成功
            try {
                //进行同步业务操作
                ServiceFuture serviceFuture = new ServiceFuture(timeout, timeUnit);
                if (service == null) {
                    return false;
                }
                serviceFuture.bind(service, objects);
                if (serviceFuture.waitServiceResult()) { //业务操作成功
                    request.setBody(null);
                    request.setReqHandlerType(RequestHandlerType.PUBLISH.value);
                    if (send(request)) {
                        result = true;
                    }
                }
                else { //业务操作时报 通知broker删除缓存
                    request.setType(TransferType.REPLY.value);
                    send(request);
                }
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while waiting for service result.");
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        //解绑业务操作
        this.service = null;
        this.objects = null;
        return result;
    }

    public String getMessageId(int seqId) {

        return client.getLocalAddress().toString() + Thread.currentThread().toString() + seqId;
    }

    //绑定业务操作
    public void bindService(Service service, Object... objects) {

        this.service = service;
        this.objects = objects;
    }

    public void stop() {

        client.stop();
    }
}
