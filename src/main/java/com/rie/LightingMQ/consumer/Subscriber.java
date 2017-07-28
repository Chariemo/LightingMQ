package com.rie.LightingMQ.consumer;

import com.rie.LightingMQ.message.Topic;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Charley on 2017/7/21.
 */
public abstract class Subscriber {

    private final static int DEFAULT_TOPICS_NUM = 10;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private Map<String, AtomicInteger> topics = new HashMap<>(DEFAULT_TOPICS_NUM);  //记录各订阅主题及各自readCounter
    private volatile boolean inOrder = false; //是否进行顺序消费
    private Consumer consumer;

    public Subscriber(Consumer consumer) {

        this.consumer = consumer;
    }

    public void subscribeTopic(String... subTopics) {   //订阅主题

        writeLock.lock();
        try {
            if (null != subTopics) {
                HashSet<String> tmp = new HashSet<>(subTopics.length);
                for (String topicName : subTopics) {
                    if (StringUtils.isNotBlank(topicName) && !topics.containsKey(topicName)) {
                        topics.put(topicName, new AtomicInteger(0));
                        tmp.add(topicName);
                    }
                }
                // 同步至消费者组
                consumer.addSubTopic(tmp);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void unSubscribeTopic(String... unSubTopics) {  //退订主题

        writeLock.lock();
        try {
            if (null != unSubTopics) {
                HashSet<String> tmp = new HashSet<>(unSubTopics.length);
                for (String topicName : unSubTopics) {
                    if (topics.containsKey(topicName)) {
                        topics.remove(topicName);
                        tmp.add(topicName);
                    }
                }
                consumer.delSubTopic(tmp);
            }
        } finally {
            writeLock.unlock();
        }
    }


    public boolean isSubscrib(String topicName) {

        boolean result = false;
        readLock.lock();
        try {
            if (!topics.isEmpty() && topics.containsKey(topicName)) {
                result = true;
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    public Map<String, AtomicInteger> getTopics() {
        return topics;
    }

    public void pull() { //主动pull所有订阅主题

        List<Topic> rtTopics = null;
        if (null != topics && !topics.isEmpty()) {
            rtTopics = consumer.fetch(topics, inOrder);
            if (null != rtTopics) {
                for (Topic topic : rtTopics) {
                    if (inOrder) {
                        if (topics.get(topic.getTopicName()).get() == 0) {
                            topics.get(topic.getTopicName()).set(topic.getReadCounter() + 1);
                        }
                        else {
                            topics.get(topic.getTopicName()).incrementAndGet();
                        }
                    }
                    notify(topic);
                }
            }
        }

    }

    public void pull(String topicName) { //pull指定订阅主题

        if (StringUtils.isNotBlank(topicName) && topics.containsKey(topicName)) {

            Map<String, AtomicInteger> tempMap = new HashMap<>(1);
            tempMap.put(topicName, topics.get(topicName));
            List<Topic> rtTopics = consumer.fetch(tempMap, inOrder);
            if (null != rtTopics) {
                notify(rtTopics.get(0));
            }
        }
    }

    public boolean isInOrder() {
        return inOrder;
    }

    public void setInOrder(boolean inOrder) {
        this.inOrder = inOrder;
    }

    public abstract void notify(Topic topic);

}
