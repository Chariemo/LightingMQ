package com.rie.LightingMQ.config;

import java.io.File;
import java.util.Properties;

/**
 * Created by Charley on 2017/7/17.
 */
public class ServerConfig extends Config{

    private final static String DEFAULT_SERVER_CONFIF = "lightingMQ.properties";

    public ServerConfig(Properties properties) {
        super(properties);
    }

    public ServerConfig(String filePath) {
        super(filePath);
    }

    public ServerConfig(File confFile) {
        super(confFile);
    }

    //默认服务器配置文件
    public static ServerConfig getDefaultServerConfig() {

        String configPath = Thread.currentThread().getContextClassLoader().getResource(DEFAULT_SERVER_CONFIF).getPath();
        return new ServerConfig(configPath);
    }

    public int getPort() {
        return getInt("LMQ.port", 9010);
    }

    public String getHost() {
        return getString("LMQ.host", null);
    }

    //数据目录
    public String getDataDir() {

        return getString("LMQ.dataDir", null);
    }

    //客户端最长无响应时间
    public int getReadIdleTime() {
        return getInt("readIdleTime", 20000);
    }

}
