package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Charley on 2017/7/18.
 */
public class DefaultFetchRequestHandler implements RequestHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFetchRequestHandler.class);

    @Override
    public Message requestHandle(Message request) {

        Message response = Message.newResponseMessage();
        System.out.println("get request: " + request.getSeqId());
        List<Topic> list = request.getBody();
        for (Topic topic : list) {
            System.out.println("sub: " + topic.getTopicName());
            topic.addContent("yes i do");
        }
        response.setBody(list);
        response.setSeqId(request.getSeqId());
        return response;
    }
}
