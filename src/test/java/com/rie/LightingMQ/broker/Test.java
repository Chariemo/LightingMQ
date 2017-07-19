package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.connection.Client;
import com.rie.LightingMQ.connection.ResponseFuture;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/18.
 */
public class Test {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) {

        Thread server = new ServerThread();
        Thread client = new ClientThread();
        server.start();
        client.start();
        try {
            server.join();
            client.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    static class ServerThread extends Thread {

        public void run() {

            Properties config = new Properties();
            config.setProperty("LMQ.port", "6789");
            Server server = Server.newServerInstance(config);
        }
    }

    static class ClientThread extends Thread {

        public void run() {

            Client client = Client.newClientInstance("localhost", 6789);
            Message request = Message.newRequestMessage();

            List<Topic> topics = new ArrayList<>();
            Topic topic = new Topic("test");
            topic.addContent("hello boy!");
            topics.add(topic);
            request.setBody(topics);

            request.setReqHandlerType(RequestHandlerType.PRODUCE.value);
            ResponseFuture response = client.write(request);
            try {
                System.out.println(response.waitResponse(2, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}