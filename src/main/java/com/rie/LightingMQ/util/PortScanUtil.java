package com.rie.LightingMQ.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Created by Charley on 2017/7/18.
 */
public class PortScanUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortScanUtil.class);

    public static boolean checkAvailablePort(int port) {

        boolean result = false;
        if (port < 1 || port > 65535) {
            return result;
        }
        ServerSocket serverSocket = null;

        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            LOGGER.warn("the port:{} has been used.", port);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (serverSocket != null) {
                result = true;
                Closer.closeQuietly(serverSocket);
            }
            return result;
        }
    }
}
