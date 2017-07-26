package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.storage.TopicQueue;
import com.rie.LightingMQ.storage.TopicQueuePool;
import com.rie.LightingMQ.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Charley on 2017/7/18.
 */
public class DefaultPublishRequestHandler implements RequestHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPublishRequestHandler.class);

    @Override
    public Message requestHandle(Message request) {

        List<Topic> contents = request.getBody();
        String messageId = request.getId();
        Message response = null;

        if (null == messageId && contents != null) {

            if (store(contents)) {
                response = Message.newResponseMessage();
                response.setSeqId(request.getSeqId());
            }
            else {
                response = Message.newExceptionMessage();
            }
        }
        else if (DefaultPrePublishRequestHandler.PRE_CACHE.containsKey(messageId)) {

            if (!store(DefaultPrePublishRequestHandler.PRE_CACHE.get(messageId))) {
                response = Message.newExceptionMessage();
            }
            else {
                DefaultPrePublishRequestHandler.PRE_CACHE.remove(messageId);
                response = Message.newResponseMessage();
                response.setSeqId(request.getSeqId());
            }
        }

        return response;
    }

    private boolean store(List<Topic> topics) {

        boolean result = true;

        for (Topic topic : topics) {
            TopicQueue topicQueue = TopicQueuePool.getOrCreateTopicQueue(topic.getTopicName());
            if (!topicQueue.offer(DataUtil.serialize(topic))) {
                result = false;
                break;
            }
        }
        return result;
    }
}