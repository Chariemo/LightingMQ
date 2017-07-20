package com.rie.LightingMQ.producer;

import com.rie.LightingMQ.message.Message;

/**
 * Created by Charley on 2017/7/20.
 */
public class SendHelperImpl implements SendHelper {

    @Override
    public Message prePublish(Message request) {
        return null;
    }

    @Override
    public void confirmPublish(Message request) {

    }

    @Override
    public boolean getServiceStatus() {
        return false;
    }
}
