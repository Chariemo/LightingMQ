package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.producer.Producer;

/**
 * Created by Charley on 2017/7/20.
 */
public class ClientTest {

    public static void main(String[] args) throws InterruptedException {

        Producer producer = Producer.newProducer();
        Topic topic = new Topic("yo");
        topic.addContent("charley");
        if (producer.publish(topic)) {
            System.out.println("ok");
        }

        Thread.currentThread().join();
    }
}
