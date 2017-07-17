package com.rie.LightingMQ.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Charley on 2017/7/17.
 */
public class Topic implements Serializable {

    private String topicName;
    private int readCounter;
    private List<Serializable> contents = new ArrayList<Serializable>();

    public Topic() {

    }

    public Topic(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadCounter() {
        return readCounter;
    }

    public void setReadCounter(int readCounter) {
        this.readCounter = readCounter;
    }

    public List<Serializable> getContents() {
        return contents;
    }

    public void addContent(Serializable content) {

        if (content != null) {
            contents.add(content);
        }
    }

    @Override
    public String toString() {

        return "topic: " + topicName + " counter: " + readCounter + " contents: " + contents;
    }
}
