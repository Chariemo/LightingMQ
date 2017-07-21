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
}
