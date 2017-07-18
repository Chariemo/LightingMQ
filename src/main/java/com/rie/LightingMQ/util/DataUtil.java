package com.rie.LightingMQ.util;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.marshalling.MarshallingEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.CRC32;

/**
 * Created by Charley on 2017/7/17.
 */
public class DataUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataUtil.class);

    /*对象序列化*/
    public static byte[] serialize(Object object) {

        byte[] result = null;
        ObjectOutputStream oos = null;
        ByteArrayOutputStream bos = null;

        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(object);
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }

    /*对象反序列化*/
    public static Object deserialize(byte[] bytes) {

        Object result = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;

        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            result = ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return result;
    }

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
}
