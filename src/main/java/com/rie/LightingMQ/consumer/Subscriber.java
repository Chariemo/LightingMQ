package com.rie.LightingMQ.consumer;

import com.rie.LightingMQ.message.Topic;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
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
    private Set<String> topics = new HashSet<>(DEFAULT_TOPICS_NUM);
    private Consumer consumer;

    public Subscriber(Consumer consumer) {

        this.consumer = consumer;
    }

    public void subscribeTopic(String... subTopics) {

        writeLock.lock();
        try {
            if (null != subTopics) {
                HashSet<String> tmp = new HashSet<>(subTopics.length);
                for (String topicName : subTopics) {
                    if (StringUtils.isNotBlank(topicName) && !topics.contains(topicName)) {
                        topics.add(topicName);
                        tmp.add(topicName);
                    }
                }
                consumer.addSubTopic(tmp);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void unSubscribeTopic(String... unSubTopics) {

        writeLock.lock();
        try {
            if (null != unSubTopics) {
                HashSet<String> tmp = new HashSet<>(unSubTopics.length);
                for (String topicName : unSubTopics) {
                    if (topics.contains(topicName)) {
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
            if (!topics.isEmpty() && topics.contains(topicName)) {
                result = true;
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void pull() {

        List<Topic> rtTopics = null;
        if (null != topics && !topics.isEmpty()) {
            rtTopics = consumer.fetch(topics.toArray(new String[0]));
            if (null != rtTopics) {
                for (Topic topic : rtTopics) {
                    notify(topic);
                }
            }
        }

    }

    public void pull(String topicName) {

        if (StringUtils.isNotBlank(topicName) && topics.contains(topicName)) {

            List<Topic> rtTopics = consumer.fetch(new String[]{topicName});
            if (null != rtTopics) {
                notify(rtTopics.get(0));
            }
        }
    }

    public abstract void notify(Topic topic);

}
