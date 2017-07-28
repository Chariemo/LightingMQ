package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.producer.Producer;
import com.rie.LightingMQ.producer.Service;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/20.
 */
public class ProducerTest implements Service{

    public static void main(String[] args) throws InterruptedException {

        new ProducerTest().test();
    }

    public boolean test() throws InterruptedException {

        Producer producer = Producer.newProducer();
        int i = 0;
        while (i < 100) {
            Topic topic = new Topic("test");
            topic.addContent("content-" + i);
            producer.bindService(this, "sleep 1 second");
            if (producer.safePublish(12, TimeUnit.SECONDS, topic)) {
                i++;
                System.out.println("send: " + topic);
                System.out.println("ok>>>>>>>>>>>>>>>>>>>>>>");
            }
            else {
                System.out.println("failed>>>>>>>>>>>>>>>>>>>");
            }
        }
        producer.stop();
        return false;
    }

    @Override
    public boolean service(Object... objects) throws InterruptedException {

        System.out.println("service: " + objects[0]);
        TimeUnit.SECONDS.sleep(1);
        return true;
    }
}
