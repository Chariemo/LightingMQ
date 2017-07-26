package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.consumer.Consumer;
import com.rie.LightingMQ.consumer.Subscriber;
import com.rie.LightingMQ.message.Topic;

/**
 * Created by Charley on 2017/7/21.
 */
public class ConsumerTest {

    public static void main(String[] args) {

        Consumer consumer = Consumer.getConsumerInstance();
        Subscriber subscriber = new Subscriber(consumer) {
            @Override
            public void notify(Topic topic) {
                System.out.println("read: " + topic);
            }
        };
        subscriber.subscribeTopic("test");
        consumer.addSubscriber(subscriber);
        consumer.startup();
//        subscriber.pull();
    }
}
