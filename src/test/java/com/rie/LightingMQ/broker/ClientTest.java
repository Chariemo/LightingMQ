package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.producer.Producer;
import com.rie.LightingMQ.producer.Service;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by Charley on 2017/7/20.
 */
public class ClientTest implements Service{

    public static void main(String[] args) throws InterruptedException {

        new ClientTest().test();
    }

    public boolean test() throws InterruptedException {

        Producer producer = Producer.newProducer();
        Topic topic = new Topic("yo");
        topic.addContent("charley");
//        producer.bindService(this, "hello");
        if (producer.safePublish(12, TimeUnit.SECONDS, topic)) {
            System.out.println("ok>>>>>>>>>>>>>>>>>>>>>>");
        }
        else {
            System.out.println("failed>>>>>>>>>>>>>>>>>>>");
        }
        producer.unsafePublish(topic);
        producer.stop();
        return false;
    }

    @Override
    public boolean service(Object... objects) throws InterruptedException {

        System.out.println("here: " + objects[0]);
        TimeUnit.SECONDS.sleep(10);
        return true;
    }
}
