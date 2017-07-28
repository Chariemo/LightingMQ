package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.consumer.Consumer;
import com.rie.LightingMQ.consumer.Subscriber;
import com.rie.LightingMQ.message.Topic;

import java.util.concurrent.TimeUnit;

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
        subscriber.setInOrder(true);
        consumer.addSubscriber(subscriber);
//        consumer.startup();
        int i = 0;
        try {
            while (i < 5) {
                subscriber.pull();
                TimeUnit.MILLISECONDS.sleep(500);
                i++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        consumer.stop();
    }
}
