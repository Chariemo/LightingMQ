package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.message.Message;

/**
 * Created by Charley on 2017/7/18.
 */
public interface RequestHandler {

    public Message requestHandle(Message request);
}
