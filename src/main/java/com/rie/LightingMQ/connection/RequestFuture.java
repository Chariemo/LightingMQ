package com.rie.LightingMQ.connection;

import com.rie.LightingMQ.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/18.
 */
public class RequestFuture {

    private final CountDownLatch valve = new CountDownLatch(1);
    private int id;
    private volatile Message response;
    private volatile boolean succeed_send;
    private Throwable cause;

    public RequestFuture() {

    }

    public RequestFuture(int id) {

        this.id = id;
    }



    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public Message waitResponse(long time, TimeUnit timeUnit) throws InterruptedException {

        valve.await(time, timeUnit);
        return response;
    }

    public void release() {

        valve.countDown();
    }
}
