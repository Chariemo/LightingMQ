package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.storage.TopicQueue;
import com.rie.LightingMQ.storage.TopicQueuePool;
import com.rie.LightingMQ.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Charley on 2017/7/18.
 */
public class DefaultFetchRequestHandler implements RequestHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFetchRequestHandler.class);

    @Override
    public Message requestHandle(Message request) {

        Message response = Message.newResponseMessage();
        List<Topic> list = request.getBody();
        List<Topic> resList = new ArrayList<>(list.size());
        for (Topic topic : list) {
            Topic temp = null;
            TopicQueue topicQueue = TopicQueuePool.getTopicQueue(topic.getTopicName());
            if (topicQueue != null) {
                byte[] bytes = topicQueue.poll();
                if (bytes != null) {
                    temp = (Topic) DataUtil.deserialize(bytes);
                }
            }

            if (temp != null) {
                resList.add(temp);
            }
            else {
                System.out.println("null");
            }
        }
        response.setBody(resList);
        response.setSeqId(request.getSeqId());
        return response;
    }
}
