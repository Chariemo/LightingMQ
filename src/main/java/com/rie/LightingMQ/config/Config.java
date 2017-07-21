package com.rie.LightingMQ.config;

import com.rie.LightingMQ.util.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Charley on 2017/7/17.
 */
public class Config {

    private final static Logger LOGGER = LoggerFactory.getLogger(Config.class);
    protected Properties properties;

    public Config(Properties properties) {

        this.properties = properties;
    }

    public Config(String filePath) {

        this(new File(filePath));
    }

    public Config(File confFile) {

        properties = new Properties();
        FileInputStream fis = null;

        try {
            fis = new FileInputStream(confFile);
            properties.load(fis);

        } catch (FileNotFoundException e) {
            LOGGER.warn("properties file is missing.");
            throw new RuntimeException(e.getMessage(), e);
        } catch (IOException e) {
            LOGGER.warn("something wrong while loading properties file.");
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            Closer.closeQuietly(fis);
        }
    }

    public String getString(String propertyName) {

        if (properties.containsKey(propertyName)) {
            return properties.getProperty(propertyName);
        }
        else {
            throw new IllegalArgumentException("required property " + propertyName + " is missing");
        }
    }

    public String getString(String propertyName, String defaultValue) {

        return properties.containsKey(propertyName) ? properties.getProperty(propertyName) : defaultValue;
    }

    public int getInt(String propertyName) {

        if (properties.containsKey(propertyName)) {
            getInt(propertyName, -1);
        }
        throw new IllegalArgumentException("required property " + propertyName + " is missing");
    }

    public int getInt(String propertyName, int defaultValue) {

        return getIntInRange(propertyName, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public int getIntInRange(String propertyName, int defaulValue, int min, int max) {

        int result = defaulValue;
        if (properties.containsKey(propertyName)) {
            result = Integer.valueOf((String) properties.get(propertyName));
        }
        if (result >= min && result <= max) {
            return result;
        }
        else {
            throw new IllegalArgumentException("the value(" + result + ") of property (" + propertyName + "" +
                    ") is not in the range");
        }
    }

    public boolean getBoolean(String propertyName) {

        if (properties.containsKey(propertyName)) {
            return "true".equalsIgnoreCase(properties.getProperty(propertyName));
        }
        throw new IllegalArgumentException("required property " + propertyName + " is missing");
    }

    public boolean getBoolean(String propertyName, boolean defaultValue) {

        return properties.containsKey(propertyName) ? "true".equalsIgnoreCase(properties.getProperty(propertyName)) : defaultValue;
    }
}
