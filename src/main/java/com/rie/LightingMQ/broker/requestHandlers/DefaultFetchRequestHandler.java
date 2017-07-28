package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.storage.TopicQueue;
import com.rie.LightingMQ.storage.TopicQueuePool;
import com.rie.LightingMQ.util.DataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.midi.Soundbank;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Charley on 2017/7/18.
 */
public class DefaultFetchRequestHandler implements RequestHandler{

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFetchRequestHandler.class);

    @Override
    public Message requestHandle(Message request) {

        Message response = null;
        List<Topic> list = request.getBody();
        List<Topic> resList = new ArrayList<>(list.size());
        for (Topic topic : list) {
            //获取队列
            TopicQueue topicQueue = TopicQueuePool.getTopicQueue(topic.getTopicName());
            if (null != topicQueue) {
                byte[] bytes = null;
                Topic temp = null;
                //按照consumer需求顺序获取
                if (topic.isOrder() && topic.getReadCounter() > 0) {
                    bytes = topicQueue.offsetRead(topic.getReadCounter());
                    if (bytes != null) {
                        temp = (Topic) DataUtil.deserialize(bytes);
                        temp.setReadCounter(topic.getReadCounter());
                    }
                }
                else { //根据当前队列的readerIndex消费
                    bytes = topicQueue.poll();
                    if (null != bytes) {
                        temp = (Topic) DataUtil.deserialize(bytes);
                        temp.setReadCounter(topicQueue.getIndex().getReadCounter());
                    }
                }

                if (temp != null) { //获取成功
                    resList.add(temp);
                }
            }
        }
        if (!resList.isEmpty()) {
            response = Message.newResponseMessage();
            response.setBody(resList);
            response.setSeqId(request.getSeqId());
        }
        else { //获取失败
            response = Message.newExceptionMessage();
            response.setSeqId(request.getSeqId());
        }
        return response;

    }
}
