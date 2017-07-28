package com.rie.LightingMQ.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Charley on 2017/7/17.
 */
public class Topic implements Serializable {

    private final static int DEFAULT_CONTENTS_NUM = 20;
    private String topicName;
    private int readCounter;  //已读取总数量
    private boolean order = false;  //是否按offset自定义消费
    private List<Serializable> contents = new ArrayList<>(DEFAULT_CONTENTS_NUM);

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

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    @Override
    public String toString() {

        return "topic: " + topicName + " readCounter: " + readCounter + " contents: " + contents;
    }
}
