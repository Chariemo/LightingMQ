package com.rie.LightingMQ.config;

import java.io.File;
import java.util.Properties;

/**
 * Created by Charley on 2017/7/17.
 */
public class ServerConfig extends Config{

    public ServerConfig(Properties properties) {
        super(properties);
    }

    public ServerConfig(String filePath) {
        super(filePath);
    }

    public ServerConfig(File confFile) {
        super(confFile);
    }

    public int getPort() {
        return getInt("LMQ.port", 9010);
    }

    public String getHost() {
        return getString("LMQ.host", null);
    }


}
