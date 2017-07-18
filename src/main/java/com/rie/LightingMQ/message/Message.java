package com.rie.LightingMQ.message;

import com.rie.LightingMQ.util.DataUtil;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * frame{head:[type(byte) + reqType(short) + seqId(int)] [body] [CRC32]} check with CRC32
 * Created by Charley on 2017/7/17.
 */
public class Message {

    private final static AtomicInteger SEQ = new AtomicInteger(1);
    public final static int HEAD_LEN = 1 + 2 + 4;
    public final static int CRC_LEN = 4;
    public final static int BODY_MAX_LEN = Integer.MAX_VALUE - HEAD_LEN - CRC_LEN;
    private byte type; // CALL | REPLY | EXCEPTION
    private short reqHandlerType; // PRODUCER | FETCH
    private int seqId;
    private transient byte[] body;

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

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getBodyLen() {

        return this.body == null ? 0 : body.length;
    }

    public int getTotalLen() {

        return this.HEAD_LEN + getBodyLen() + this.CRC_LEN;
    }

    public void writeToByteBuf(ByteBuf byteBuf) {

        if (byteBuf != null) {

            //write message head
            byteBuf.writeByte(this.type);
            byteBuf.writeShort(this.reqHandlerType);
            byteBuf.writeInt(this.seqId);

            //write message body
            if (getBodyLen() > 0) {
                byteBuf.writeBytes(this.body);
            }

            //write CRC
            long crc32 = DataUtil.calculateCRC(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes() - 4);
            byteBuf.writeLong(crc32);
        }
    }

    public void readFromByteBuf(ByteBuf byteBuf) {

        if (byteBuf != null) {
            int len = byteBuf.readableBytes() - CRC_LEN;
            long crc32 = DataUtil.calculateCRC(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes() - 4);

            //read message head
            byte type = byteBuf.readByte();
            short reqHandlerType = byteBuf.readShort();
            int seqId = byteBuf.readInt();

            //read message body
            int body_len = len - HEAD_LEN;
            byte[] body = new byte[body_len];
            byteBuf.readBytes(body);

            //CRC CHECK
            long crcRead = byteBuf.readUnsignedInt();
            if (crcRead != crc32) {
                byteBuf.discardReadBytes();
                byteBuf.release();
                throw new RuntimeException("CRC WRONG");
            }

            this.setType(type);
            this.setReqHandlerType(reqHandlerType);
            this.setSeqId(seqId);
            this.setBody(this.body);

            byteBuf.release();
        }
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == Message.class) {
            Message message = (Message)obj;
            return this.type == message.type && this.reqHandlerType == message.reqHandlerType
                    && this.seqId == message.seqId && getBodyLen() == message.getBodyLen();
        }
        else {
            return false;
        }
    }

    @Override
    public String toString() {

        return "type: " + this.type + " reqHandler: " + this.reqHandlerType + " " +
                "reqId: " + this.seqId;
    }
}
