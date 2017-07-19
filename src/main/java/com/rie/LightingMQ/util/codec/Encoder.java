package com.rie.LightingMQ.util.codec;

import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.util.DataUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.marshalling.MarshallerProvider;
import io.netty.handler.codec.marshalling.MarshallingEncoder;

/**
 * Created by Charley on 2017/7/18.
 */
public class Encoder extends MarshallingEncoder{

    public Encoder(MarshallerProvider provider) {
        super(provider);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf byteBuf) throws Exception {

        super.encode(ctx, msg, byteBuf);
        long crc32 = DataUtil.calculateCRC(byteBuf, byteBuf.readerIndex(), byteBuf.readableBytes());
        byteBuf.writeLong(crc32);
    }
}
