package com.rie.LightingMQ.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by Charley on 2017/7/20.
 */
public class ServiceFuture {

    private final static Logger LOGGER = LoggerFactory.getLogger(ServiceFuture.class);
    private final CountDownLatch valve = new CountDownLatch(1);
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean result = false;
    private final long timeOut;
    private final TimeUnit timeUnit;
    private Service service;
    private Object[] vars;

    public ServiceFuture(long timeOut, TimeUnit timeUnit) {

        this.timeOut = timeOut;
        this.timeUnit = timeUnit;
    }

    public void bind(Service service, Object... objects) {

        this.service = service;
        this.vars = objects;

        executor.execute(new serviceTask());
    }

    public boolean waitServiceResult() throws InterruptedException {

        valve.await(timeOut, timeUnit);
        executor.shutdownNow();
        return result;
    }

    private class serviceTask implements Runnable {

        @Override
        public void run() {

            //业务处理
            try {
                result = service.service(vars);
            } catch (InterruptedException e) {
                LOGGER.warn("service timeout and been interrupted.");
            }
            valve.countDown();
        }
    }
}
