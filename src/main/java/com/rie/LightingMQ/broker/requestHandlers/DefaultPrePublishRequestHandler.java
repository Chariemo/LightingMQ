package com.rie.LightingMQ.broker.requestHandlers;

import com.rie.LightingMQ.broker.RequestHandler;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.Topic;
import com.rie.LightingMQ.storage.TopicQueuePool;
import com.rie.LightingMQ.util.DataUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Charley on 2017/7/20.
 */
public class DefaultPrePublishRequestHandler implements RequestHandler{

    protected static final Map<String, List<Topic>> PRE_CACHE = new ConcurrentHashMap<>();

    @Override
    public Message requestHandle(Message request) {

        Message response = null;
        if (null != request.getBody()) { //预发布 缓存
            if (!PRE_CACHE.containsKey(request.getId())) { //防止重发导致重复入库
                PRE_CACHE.put(request.getId(), request.getBody());
            }
            response = Message.newResponseMessage();
            response.setSeqId(request.getSeqId());
        }
        else {
            response = Message.newExceptionMessage();
        }

        return response;
    }
}
