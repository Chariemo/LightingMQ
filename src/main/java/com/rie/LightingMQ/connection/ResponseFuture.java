package com.rie.LightingMQ.connection;

import com.rie.LightingMQ.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/18.
 */
public class ResponseFuture {

    private final CountDownLatch valve = new CountDownLatch(1);
    private int id;
    private Throwable cause;
    private volatile Message response;
    private volatile boolean succeed_send;
    private volatile boolean succeed_recieved;


    public ResponseFuture() {

    }

    public ResponseFuture(int id) {

        this.id = id;
    }

    public boolean isSucceed_recieved() {
        return succeed_recieved;
    }

    public void setSucceed_recieved(boolean succeed_recieved) {
        this.succeed_recieved = succeed_recieved;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public void setResponse(Message response) {
        this.response = response;
    }

    public boolean isSucceed_send() {
        return succeed_send;
    }

    public void setSucceed_send(boolean succeed_send) {
        this.succeed_send = succeed_send;
    }

    public Message waitResponse(long time, TimeUnit timeUnit) throws InterruptedException {

        valve.await(time, timeUnit);
        return response;
    }

    public void release() {

        valve.countDown();
    }
}
