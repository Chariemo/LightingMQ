package com.rie.LightingMQ.producer;

/**
 * Created by Charley on 2017/7/19.
 */
public interface Service {

    boolean service(Object... objects) throws InterruptedException;
}
