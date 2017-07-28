package com.rie.LightingMQ.util;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.marshalling.MarshallingEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/**
 * Created by Charley on 2017/7/17.
 */
public class DataUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataUtil.class);

    public static byte[] serialize(Object object) {

        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            // 序列化
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            LOGGER.error("serialize object ({}) wrong.", object);
        }
        return null;
    }

    public static Object deserialize(byte[] bytes) {

        ByteArrayInputStream bais = null;
        try {
            // 反序列化
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            LOGGER.error("deserialize to object wrong.");
        }
        return null;
    }

    // 计算CRC32 校验码
    public static long calculateCRC(ByteBuf data, int offset, int len) {

        long result = 0L;
        CRC32 crc32 = new CRC32();
        if (data.hasArray()) {
            crc32.update(data.array(), data.arrayOffset() + offset, len);
        }
        else {
            for (int i = 0; i < len; ++i) {
                crc32.update(data.getByte(i + offset));
            }
        }
        result = crc32.getValue();
        crc32.reset();

        return result;
    }

    public static byte[] uint32ToBytes(long value) {
        return uint32ToBytes(ByteOrder.BIG_ENDIAN, value);
    }

    public static byte[] uint32ToBytes(ByteOrder order, long value) {
        byte[] buf = new byte[4];
        uint32ToBytes(order, buf, 0, value);
        return buf;
    }

    private static void uint32ToBytes(ByteOrder order, byte[] buf, int offset, long value) {
        if (offset + 4 > buf.length) throw new IllegalArgumentException("buf no has 4 byte space");
        if (order == ByteOrder.BIG_ENDIAN) {
            buf[offset + 0] = (byte) (0xff & (value >>> 24));
            buf[offset + 1] = (byte) (0xff & (value >>> 16));
            buf[offset + 2] = (byte) (0xff & (value >>> 8));
            buf[offset + 3] = (byte) (0xff & (value));
        } else {
            buf[offset + 0] = (byte) (0xff & (value));
            buf[offset + 1] = (byte) (0xff & (value >>> 8));
            buf[offset + 2] = (byte) (0xff & (value >>> 16));
            buf[offset + 3] = (byte) (0xff & (value >>> 24));
        }
    }
}
