package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Charley on 2017/7/18.
 */
public class DefaultProduceRequestHandler implements RequestHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProduceRequestHandler.class);

    @Override
    public Message requestHandle(Message request) {

        Message response = Message.newResponseMessage();
        System.out.println(request.getBody().get(0).getContents().get(0).toString());
        response.setSeqId(request.getSeqId());
        return response;
    }
}
