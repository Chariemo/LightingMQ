package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.storage.TopicQueuePool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Charley on 2017/7/20.
 */
public class ServerTest {

    public static void main(String[] args) {

        Server.newServerInstance();
    }
}
