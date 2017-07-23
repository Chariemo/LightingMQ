package com.rie.LightingMQ.consumer;

import com.rie.LightingMQ.broker.RequestHandlerType;
import com.rie.LightingMQ.config.ConnectionConfig;
import com.rie.LightingMQ.connection.Client;
import com.rie.LightingMQ.connection.RequestFuture;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.message.TransferType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    private static final ExecutorService THREAD_POOL = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private Client client;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = readWriteLock.readLock();
    private Lock writeLock = readWriteLock.writeLock();
    private Set<Subscriber> subscribers = new HashSet<>(DEFAULT_SUBSCRIBER_NUM);
    private Set<String> topics = new HashSet<>(DEFAULT_TOPICS_NUM);

    public Consumer() {

    }

    public static Consumer newConsumerInstance(ConnectionConfig config) {

        Consumer consumer = new Consumer();
        consumer.setClient(Client.newClientInstance(config));
        return consumer;
    }

    public static Consumer newConsumerInstance() {

        return newConsumerInstance(ConnectionConfig.getDefaultConConfig());
    }

    public static Consumer newConsumerInstance(String configPath) {

        return newConsumerInstance(new ConnectionConfig(configPath));
    }

    public static Consumer newConsumerInstance(Properties properties) {

        return newConsumerInstance(new ConnectionConfig(properties));
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void fetch() {

        List<Topic> rtTopics = null;
        topics.add("1");
        topics.add("2");
        if (!topics.isEmpty()) {
            rtTopics = fetch(topics.toArray(new String[0]));
        }

        if (rtTopics != null) {
            //TODO
            for (Topic topic : rtTopics) {
                System.out.println(topic);
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
            request.setBody(requestTopics);

            RequestFuture requestFuture = client.write(request);
            try {
                response = requestFuture.waitResponse(10, TimeUnit.SECONDS);
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

    private class Notice implements Callable {

        private List<Topic> rtTopic;

        public Notice(List<Topic> rtTopic) {

            this.rtTopic = rtTopic;
        }

        @Override
        public Object call() throws Exception {

            return null;
        }
    }

}
