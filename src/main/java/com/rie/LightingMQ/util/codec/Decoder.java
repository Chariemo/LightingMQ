package com.rie.LightingMQ.util.codec;

import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.util.DataUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.marshalling.MarshallingDecoder;
import io.netty.handler.codec.marshalling.UnmarshallerProvider;

/**
 * Created by Charley on 2017/7/18.
 */
public class Decoder extends MarshallingDecoder{

    public Decoder(UnmarshallerProvider provider) {
        super(provider);
    }

    public Decoder(UnmarshallerProvider provider, int maxObjectSize) {
        super(provider, maxObjectSize);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {

        if (byteBuf != null) {
            long crc32 = DataUtil.calculateCRC(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes() - Message.CRC_LEN);

            //CRC CHECK
            byteBuf.readerIndex(byteBuf.readableBytes() - Message.CRC_LEN);
            long crcRead = byteBuf.readLong();
            if (crcRead != crc32) {
                byteBuf.discardReadBytes();
                byteBuf.release();
                throw new RuntimeException("CRC WRONG");
            }
        }
        byteBuf.readerIndex(0);
        byteBuf.writerIndex(byteBuf.readableBytes() - Message.CRC_LEN);
        return super.decode(ctx, byteBuf);
    }
}
