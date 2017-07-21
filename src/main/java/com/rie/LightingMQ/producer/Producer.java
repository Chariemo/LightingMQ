package com.rie.LightingMQ.producer;

import com.rie.LightingMQ.broker.RequestHandlerType;
import com.rie.LightingMQ.broker.Server;
import com.rie.LightingMQ.config.ServerConfig;
import com.rie.LightingMQ.connection.Client;
import com.rie.LightingMQ.connection.RequestFuture;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.message.TransferType;
import com.rie.LightingMQ.util.Closer;
import com.sun.istack.internal.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by Charley on 2017/7/19.
 */
public class Producer {

    private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    public final static String DEFAULT_CONFIG_PATH = "LightingMQ.properties";
    private Client client;
    private ServerConfig config;
    private BlockingQueue<Message> reSendQueue = new LinkedBlockingDeque<>();
    private Service service;
    private Object[] objects;

    public Producer() {

    }

    public static Producer newProducer() {

        Producer producer = null;
        BufferedInputStream bis = null;
        Properties properties = new Properties();
        ServerConfig config0 = null;

        String configPath = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_CONFIG_PATH).getPath();
        File configFile = new File(configPath);
        try {

            bis = new BufferedInputStream(new FileInputStream(configFile));
            properties.load(bis);
            config0 = new ServerConfig(properties);
        } catch (FileNotFoundException e) {
            LOGGER.warn("LightingMQ.properties is missing when create a new producer.");
            throw new RuntimeException(e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.warn("something wrong while loading LightingMQ.properties.");
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            Closer.closeQuietly(bis);
        }

        producer = new Producer();
        producer.setConfig(config0);
        producer.setClient(Client.newClientInstance(config0));
        return producer;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setConfig(ServerConfig config) {
        this.config = config;
    }

    public boolean unsafePublish(Topic topic) {

        ArrayList<Topic> list = new ArrayList<>(1);
        list.add(topic);
        return unsafePublish(list);
    }

    public boolean unsafePublish(List<Topic> topics) {

        boolean result = false;

        if (client.reConnect()) {

            Message request = Message.newRequestMessage();
            request.setReqHandlerType(RequestHandlerType.PUBLISH.value);
            request.setBody(topics);
            result = send(request);
        }

        return result;
    }

    public boolean send(Message request) {

        boolean result = false;
        RequestFuture responseFuture = client.write(request);
        Message response = null;

        try {
            response = responseFuture.waitResponse(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("interrupted while waiting for response <- {}.", request);
            throw new RuntimeException(e.getMessage(), e);
        }

        if (response == null) {
            LOGGER.warn("timeout while waiting for response <- {}.", request);
            reSendQueue.add(request);
        } else if (response.getType() == TransferType.EXCEPTION.value) {
            LOGGER.error("something error send the request({}) to server.", request);
        }
        else {
            result = true;
        }

            /*if (!reSendQueue.isEmpty()) {
                ArrayList<Topic> list = new ArrayList<>(reSendQueue);
                if (publish(list)) {
                    result = true;
                    reSendQueue.clear();
                }
            }*/
        return result;
    }

    public boolean safePublish(long timeout, TimeUnit timeUnit, Topic topic) {

        ArrayList<Topic> list = new ArrayList<>(1);
        list.add(topic);
        return safePublish(timeout, timeUnit, list);
    }

    public boolean safePublish(long timeout, TimeUnit timeUnit, List<Topic> topics) {

        boolean result = false;
        Message request = Message.newRequestMessage();
        request.setReqHandlerType(RequestHandlerType.PRE_PUBLISH.value);
        request.setBody(topics);

        if (send(request)) {
            try {
                ServiceFuture serviceFuture = new ServiceFuture(timeout, 5, timeUnit);
                if (service == null) {
                    throw new NullPointerException("service is null");
                }
                serviceFuture.bind(service, objects);
                if (serviceFuture.waitServiceResult()) {
                    request.setBody(null);
                    request.setReqHandlerType(RequestHandlerType.PUBLISH.value);
                    if (send(request)) {
                        result = true;
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while waiting for service result.");
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return result;
    }

    public void bindService(Service service, Object... objects) {

        this.service = service;
        this.objects = objects;
    }

    public void stop() {

        client.stop();
    }
}
