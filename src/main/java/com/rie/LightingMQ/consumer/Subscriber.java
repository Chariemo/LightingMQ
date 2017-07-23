package com.rie.LightingMQ.consumer;

import com.rie.LightingMQ.message.Topic;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Charley on 2017/7/21.
 */
public abstract class Subscriber {

    private final static int DEFAULT_TOPICS_NUM = 10;
    private Set<String> topics = new HashSet<>(DEFAULT_TOPICS_NUM);
    private Consumer consumer;

    public Subscriber(Consumer consumer) {

        this.consumer = consumer;
    }

    public void subscribTopic(String topicName) {

        if (!topics.contains(topicName)) {
            topics.add(topicName);
        }
    }

    public boolean isSubscrib(String topicName) {

        boolean result = false;
        if (!topics.isEmpty() && topics.contains(topicName)) {
            result = true;
        }
        return result;
    }

    public Set<String> getTopics() {
        return topics;
    }

    public void pull() {

    }

    public void pull(String topicName) {


    }

    public abstract void notify(Topic topic);

}
