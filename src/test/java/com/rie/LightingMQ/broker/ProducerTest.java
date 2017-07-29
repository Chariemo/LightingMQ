package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.producer.Producer;
import com.rie.LightingMQ.producer.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/20.
 */
public class ProducerTest implements Service, Runnable{

    private String topicName;

    public ProducerTest(String topicName) {

        this.topicName = topicName;
    }

    public void run() {
        try {
            test();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean test() throws InterruptedException {

        Producer producer = Producer.newProducer();
        int i = 1;
        while (i <= 100) {
            Topic topic = new Topic(topicName);
            topic.addContent("content-" + i);
            producer.bindService(this, "sleep 1 second");
            if (producer.safePublish(10, TimeUnit.SECONDS, topic)) {
                i++;
                System.out.println("send: " + topic);
                System.out.println("ok>>>>>>>>>>>>>>>>>>>>>>");
            }
            else {
                System.out.println("failed>>>>>>>>>>>>>>>>>>>");
            }
        }
        return false;
    }

    @Override
    public boolean service(Object... objects) throws InterruptedException {

        System.out.println("service: " + objects[0]);
        TimeUnit.SECONDS.sleep(1);
        return true;
    }


    public static void main(String[] args) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        executor.execute(new ProducerTest("test1"));
        executor.execute(new ProducerTest("test2"));
    }
}
