package com.rie.LightingMQ.broker;

import java.util.Properties;

/**
 * Created by Charley on 2017/7/20.
 */
public class ServerTest {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.setProperty("LMQ.port", "6789");
        Server.newServerInstance(config);
    }
}
