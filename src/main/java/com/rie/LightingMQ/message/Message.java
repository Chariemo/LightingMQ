package com.rie.LightingMQ.message;

import com.rie.LightingMQ.broker.RequestHandlerType;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * frame{head:[id(String) + type(byte) + reqType(short) + seqId(int)] [body] [CRC32]} check with CRC32
 * Created by Charley on 2017/7/17.
 */
public class Message implements Serializable {

    private final static AtomicInteger SEQ = new AtomicInteger(1);
    public final static int CRC_LEN = Long.BYTES;
    //head
    private String id;
    private byte type; // CALL | REPLY | EXCEPTION | HEARTBEAT
    private short reqHandlerType; // PUBLISH | FETCH
    private int seqId;
    //body
    private List<Topic> body;

    public Message() {

    }

    public static Message newRequestMessage() {

        Message message = new Message();
        message.setType(TransferType.CALL.value);
        message.setSeqId(SEQ.getAndIncrement());
        return message;
    }

    public static Message newResponseMessage() {

        Message message = new Message();
        message.setType(TransferType.REPLY.value);
        return message;
    }

    public static Message newExceptionMessage() {

        Message message = new Message();
        message.setType(TransferType.EXCEPTION.value);
        return message;
    }

    public static Message newHeartbeatMessage() {

        Message message = new Message();
        message.setType(TransferType.HEARTBEAT.value);
        return message;
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public short getReqHandlerType() {
        return reqHandlerType;
    }

    public void setReqHandlerType(short reqHandlerType) {
        this.reqHandlerType = reqHandlerType;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public int getSeqId() {
        return seqId;
    }

    public List<Topic> getBody() {
        return body;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setBody(List<Topic> body) {
        this.body = body;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == Message.class) {
            Message message = (Message)obj;
            return this.type == message.type && this.reqHandlerType == message.reqHandlerType
                    && this.seqId == message.seqId && this.body == message.body;
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {

        return "type: " + TransferType.valueOf(this.type) + " reqHandler: " + RequestHandlerType.valueOf(
                this.reqHandlerType) + " reqId: " + this.seqId;
    }
}
