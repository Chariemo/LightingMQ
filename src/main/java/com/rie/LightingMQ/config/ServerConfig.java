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


}
