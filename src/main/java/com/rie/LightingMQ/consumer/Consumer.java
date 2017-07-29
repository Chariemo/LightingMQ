package com.rie.LightingMQ.consumer;

import com.rie.LightingMQ.broker.RequestHandlerType;
import com.rie.LightingMQ.config.ConnectionConfig;
import com.rie.LightingMQ.connection.Client;
import com.rie.LightingMQ.connection.RequestFuture;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.message.TransferType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Charley on 2017/7/17.
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private static final int DEFAULT_SUBSCRIBER_NUM = 100;
    private static final int DEFAULT_TOPICS_NUM = 100;
    private static Client client;
    private static ConnectionConfig config;
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = readWriteLock.readLock(); //订阅者读取锁
    private Lock writeLock = readWriteLock.writeLock();  //订阅者写入锁
    private Set<Subscriber> subscribers = new HashSet<>(DEFAULT_SUBSCRIBER_NUM); //订阅者
    //通过当前消费者组订阅的主题以及各自订阅者数量
    private Map<String, AtomicInteger> topics = new ConcurrentHashMap<>(DEFAULT_TOPICS_NUM);
    private static final Consumer CONSUMER = new Consumer(); //单例模式

    private Consumer() {

    }

    public static Consumer getConsumerInstance(ConnectionConfig config) {

        Consumer.config = config;
        client = Client.newClientInstance(config);
        return CONSUMER;
    }

    public static Consumer getConsumerInstance() {

        return getConsumerInstance(ConnectionConfig.getDefaultConConfig());
    }

    public static Consumer getConsumerInstance(String configPath) {

        return getConsumerInstance(new ConnectionConfig(configPath));
    }

    public static Consumer getConsumerInstance(Properties properties) {

        return getConsumerInstance(new ConnectionConfig(properties));
    }

    public void fetch() {

        List<Topic> rtTopics = null;
        if (!topics.isEmpty()) {
            rtTopics = fetch(topics, false);  //消费者组按readIndex进行获取
        }

        if (rtTopics != null && !rtTopics.isEmpty()) {
            readLock.lock();
            try {
                for (Topic topic : rtTopics) {
                    for (Subscriber subscriber : subscribers) {
                        if (null != subscriber.getTopics() && subscriber.isSubscrib(topic.getTopicName())) {
                            subscriber.notify(topic); //通知各订阅者
                        }
                    }
                }
            } finally {
                readLock.unlock();
            }
        }
    }

    public List<Topic> fetch(Map<String, AtomicInteger> topicMaps, boolean inOrder) {

        Iterator<Map.Entry<String, AtomicInteger>> iterator = topicMaps.entrySet().iterator();
        List<Topic> requestTopics = new ArrayList<>(topicMaps.size());
        while (iterator.hasNext()) {
            Map.Entry<String, AtomicInteger> entry = iterator.next();
            Topic topic = new Topic(entry.getKey());
            if (inOrder) { //按照订阅者自定义顺序获取
                topic.setOrder(true);
                topic.setReadCounter(entry.getValue().get());
            }
            requestTopics.add(topic);
        }
        return fetch(requestTopics);
    }

    public List<Topic> fetch(List<Topic> requestTopics) {

        List<Topic> rtTopics = null;
        Message response = null;

        if (client.reConnect()) { //查看链接状态 或重连
            Message request = Message.newRequestMessage();
            request.setReqHandlerType(RequestHandlerType.FETCH.value);
            request.setId(getMessageId(request.getSeqId())); //设置信息唯一值 防止重发导致消息重复发布
            request.setBody(requestTopics);

            RequestFuture requestFuture = client.write(request);
            try {
                response = requestFuture.waitResponse(config.getResponseTimeOut(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while waiting for response <- {}.", request);
                throw new RuntimeException(e.getMessage(), e);
            }

            if (response == null) { //固定时间未获取到服务器响应
                LOGGER.warn("timeout while waiting for response <- {}.", request);
            }
            else if (response.getType() == TransferType.EXCEPTION.value) { //服务器获取数据异常
                LOGGER.error("something error happened for request({}) to server.", request);
                this.scheduler.shutdown();
            }
            else {
                rtTopics = response.getBody();
            }
        }
        return rtTopics;
    }

    public String getMessageId(int seqId) {

        return client.getChannel().localAddress().toString() + Thread.currentThread().toString() + seqId;
    }

    public void stop() {

        this.scheduler.shutdown();
        if (client != null) {
            client.stop();
        }
    }

    public void addSubscriber(Subscriber subscriber) {

        writeLock.lock();
        try {
            if (null != subscriber && !subscribers.contains(subscriber)) {
                subscribers.add(subscriber);
                addSubTopic(subscriber.getTopics().keySet());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void delSubscriber(Subscriber subscriber) {

        writeLock.lock();
        try {
            if (null != subscriber && subscribers.contains(subscriber)) {
                subscribers.remove(subscriber);
                delSubTopic(subscriber.getTopics().keySet());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void addSubTopic(Set<String> subTopics) {

        if (subTopics != null && !subTopics.isEmpty()) {
            for (String topicName : subTopics) {
                if (StringUtils.isNotBlank(topicName)) {
                    if (topics.containsKey(topicName)) {
                        topics.get(topicName).incrementAndGet();
                    }
                    else {
                        topics.put(topicName, new AtomicInteger(1));
                    }
                }
            }
        }
    }

    public void delSubTopic(Set<String> delTopics) {

        if (null != delTopics && !delTopics.isEmpty()) {
            for (String topicName : delTopics) {
                //改主题的订阅者为0时 从订阅队列中删除
                if (topics.containsKey(topicName) && topics.get(topicName).decrementAndGet() == 0) {
                    topics.remove(topicName);
                }
            }
        }
    }

    public void startup() {

        //主动pull
        this.scheduler.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                fetch();
            }
        }, 0, config.getConsumerPeriod(), TimeUnit.MILLISECONDS);
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }
}
