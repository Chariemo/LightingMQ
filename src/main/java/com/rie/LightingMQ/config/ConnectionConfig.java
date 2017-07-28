package com.rie.LightingMQ.config;

import java.io.File;
import java.util.Properties;

/**
 * Created by Charley on 2017/7/21.
 */
public class ConnectionConfig extends Config{

    private final static String DEFAULT_CON_CONFIG = "connection.properties";

    public ConnectionConfig(Properties properties) {
        super(properties);
    }

    public ConnectionConfig(String filePath) {
        super(filePath);
    }

    public ConnectionConfig(File confFile) {
        super(confFile);
    }

    public static ConnectionConfig getDefaultConConfig() {

        String configPath = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_CON_CONFIG).getPath();
        return new ConnectionConfig(configPath);
    }

    public int getPort() {
        return getInt("connection.port", 9010);
    }

    public String getHost() {

        return getString("connection.host", null);
    }

    //重连次数
    public int getReConnectTimes() {

        return getInt("reConnectTimes", 3);
    }

    //最长等待回复时间
    public int getResponseTimeOut() {

        return getInt("responseTimeOut", 5000);
    }

    //重发次数
    public int getReSendTimes() {

        return getInt("reSendTimes", 3);
    }

    //客户端超过最长空闲时间后发送心跳
    public int getAllIdleTime() {

        return getInt("allIdleTime", 10000);
    }

    //pull频率
    public int getConsumerPeriod() {

        return getInt("consumer.period", 60000);
    }
}
