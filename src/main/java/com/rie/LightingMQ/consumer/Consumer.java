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
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = readWriteLock.readLock();
    private Lock writeLock = readWriteLock.writeLock();
    private Set<Subscriber> subscribers = new HashSet<>(DEFAULT_SUBSCRIBER_NUM);
    private Map<String, AtomicInteger> topics = new ConcurrentHashMap<>(DEFAULT_TOPICS_NUM);
    private static final Consumer CONSUMER = new Consumer();

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
            rtTopics = fetch(topics.keySet().toArray(new String[0]));
        }

        if (rtTopics != null && !rtTopics.isEmpty()) {
            readLock.lock();
            try {
                for (Topic topic : rtTopics) {
                    for (Subscriber subscriber : subscribers) {
                        if (null != subscriber.getTopics() && subscriber.getTopics().contains(topic.getTopicName())) {
                            subscriber.notify(topic);
                        }
                    }
                }
            } finally {
                readLock.unlock();
            }
        }
    }

    public List<Topic> fetch(String[] topics) {

        return fetch(Arrays.asList(topics));
    }

    public List<Topic> fetch(List<String> topics) {

        List<Topic> rtTopics = null;
        Message response = null;

        if (client.reConnect()) {
            List<Topic> requestTopics = new ArrayList<>(topics.size());
            for (String topicName : topics) {
                Topic topic = new Topic(topicName);
                requestTopics.add(topic);
            }
            Message request = Message.newRequestMessage();
            request.setReqHandlerType(RequestHandlerType.FETCH.value);
            request.setId(getMessageId(request.getSeqId()));
            request.setBody(requestTopics);

            RequestFuture requestFuture = client.write(request);
            try {
                response = requestFuture.waitResponse(config.getResponseTimeOut(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while waiting for response <- {}.", request);
                throw new RuntimeException(e.getMessage(), e);
            }

            if (response == null) {
                LOGGER.warn("timeout while waiting for response <- {}.", request);
            }
            else if (response.getType() == TransferType.EXCEPTION.value) {
                LOGGER.error("something error happened for request({}) to server.", request);
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

        if (client != null) {
            client.stop();
        }
    }

    public void addSubscriber(Subscriber subscriber) {

        writeLock.lock();
        try {
            if (null != subscriber && !subscribers.contains(subscriber)) {
                subscribers.add(subscriber);
                addSubTopic(subscriber.getTopics());
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
                delSubTopic(subscriber.getTopics());
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
                if (topics.containsKey(topicName) && topics.get(topicName).decrementAndGet() == 0) {
                    topics.remove(topicName);
                }
            }
        }
    }

    public void startup() {

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {

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
