package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;

/**
 * Created by Charley on 2017/7/20.
 */
public class DefaultPrePublishRequestHandler implements RequestHandler{

    @Override
    public Message requestHandle(Message request) {

        Message response = Message.newResponseMessage();
        response.setSeqId(request.getSeqId());

        System.out.println("pre publish");
        return response;
    }
}
