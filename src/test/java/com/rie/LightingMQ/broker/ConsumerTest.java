package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.consumer.Consumer;
import com.rie.LightingMQ.consumer.Subscriber;
import com.rie.LightingMQ.message.Topic;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/21.
 */
public class ConsumerTest {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Consumer consumer = Consumer.getConsumerInstance();
        Subscriber subscriber1 = new Subscriber(consumer) {
            @Override
            public void notify(Topic topic) {
                System.out.println("subscriber1 read: " + topic);
            }
        };
        subscriber1.subscribeTopic("test1");
        consumer.addSubscriber(subscriber1);

        Subscriber subscriber2 = new Subscriber(consumer) {
            @Override
            public void notify(Topic topic) {
                System.out.println("subscriber2 read: " + topic);
            }
        };
        subscriber2.subscribeTopic("test2");
        consumer.addSubscriber(subscriber2);

        consumer.startup();
//        subscriber1.setInOrder(true);
//        subscriber2.setInOrder(true);
//        executor.execute(new SubscriberTask(subscriber1));
//        executor.execute(new SubscriberTask(subscriber2));
    }

    static class SubscriberTask implements Runnable {

        private Subscriber subscriber;

        public SubscriberTask(Subscriber subscriber) {

            this.subscriber = subscriber;
        }
        @Override
        public void run() {
            int i = 0;
            try {
                while (i < 20) {
                    subscriber.pull();
                    TimeUnit.MILLISECONDS.sleep(2000);
                    i++;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
