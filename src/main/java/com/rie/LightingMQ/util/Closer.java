package com.rie.LightingMQ.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by Charley on 2017/7/17.
 */
public class Closer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Closer.class);

    public static void closeQuietly(Closeable closeable, Logger logger) {

        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void closeQuietly(Closeable closeable) {

        closeQuietly(closeable, LOGGER);
    }
}
