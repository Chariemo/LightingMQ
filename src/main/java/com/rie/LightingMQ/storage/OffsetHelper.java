package com.rie.LightingMQ.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * frame{length(4B) + r/wCounter(4B) + timeStamp(8B) + fileNo(4B) + offset(4B)}
 * Created by Charley on 2017/7/27.
 */
public class OffsetHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetHelper.class);
    private static final String OFFSET_FILE_SUFFIX = ".lmq_offset";
    private static final int OFFSET_FILE_DEFAULT_SIZE = 128 * 1024 * 1024; // 128MB
    private static final int PER_SIZE = 20;
    private RandomAccessFile offsetFile;
    private FileChannel fileChannel;
    private MappedByteBuffer mappedByteBuffer;
    private ByteBuffer byteBuffer;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private Map<Integer, OffsetPOJO> offsetMap = new HashMap<>(128); //保存readCounter对应的索引信息

    public OffsetHelper(String queueName, String fileDir) {

        String offsetFilePath = formatOffsetFilePath(queueName, fileDir);
        File file = new File(offsetFilePath);
        try {
            if (file.exists()) {    //offset文件存在 直接读取
                this.offsetFile = new RandomAccessFile(file, "rw");
                fileChannel = offsetFile.getChannel();
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, OFFSET_FILE_DEFAULT_SIZE);
                byteBuffer = mappedByteBuffer.load();
                int length = byteBuffer.getInt();
                for (int i = 0; i < length; ++i) {
                    OffsetPOJO pojo = new OffsetPOJO();
                    pojo.setCounter(byteBuffer.getInt());
                    pojo.setTimeStamp(byteBuffer.getLong());
                    pojo.setFileNo(byteBuffer.getInt());
                    pojo.setOffset(byteBuffer.getInt());
                    System.out.println("offsetPOJO: " + pojo);
                    offsetMap.put(pojo.getCounter(), pojo);
                }
            }
            else {  //offset文件不存在 初始化数据
                offsetFile = new RandomAccessFile(file, "rw");
                this.offsetFile = new RandomAccessFile(file, "rw");
                fileChannel = offsetFile.getChannel();
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, OFFSET_FILE_DEFAULT_SIZE);
                byteBuffer = mappedByteBuffer.load();
                byteBuffer.putInt(0);
            }
        } catch (IOException e) {
            LOGGER.error("something wrong while create OffsetHelper.");
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String formatOffsetFilePath(String queueName, String fileDir) {

        return fileDir + File.separator + String.format("offset_%s%s", queueName, OFFSET_FILE_SUFFIX);
    }

    public void write(int counter, int fileNo, int offset) {

        writeLock.lock();
        try {
            int length = byteBuffer.getInt(0);
            int currentPos = length * PER_SIZE + Integer.BYTES;
            byteBuffer.position(currentPos);
            long timeStamp = System.currentTimeMillis();

            byteBuffer.putInt(counter);
            byteBuffer.putLong(timeStamp);
            byteBuffer.putInt(fileNo);
            byteBuffer.putInt(offset);
            byteBuffer.putInt(0, length + 1);

            OffsetPOJO pojo = new OffsetPOJO();
            pojo.setCounter(counter);
            pojo.setFileNo(fileNo);
            pojo.setTimeStamp(timeStamp);
            pojo.setOffset(offset);
            offsetMap.put(counter, pojo);
        } finally {
            writeLock.unlock();
        }
    }

    public OffsetPOJO read(int readCounter) {

        OffsetPOJO result = null;
        readLock.lock();
        try {

            if (null != offsetMap && offsetMap.containsKey(readCounter)) {
                result = offsetMap.get(readCounter);
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    public void sync() {

        if (mappedByteBuffer != null) {
            mappedByteBuffer.force();
        }
    }

    public void close() {

        if (offsetFile != null) {
            sync();
            try {
                Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
                method.setAccessible(true);
                method.invoke(FileChannelImpl.class, mappedByteBuffer);
            } catch (Exception e) {
                LOGGER.warn("close offset file ({}) failed.", offsetFile);
            }
            mappedByteBuffer = null;
            try {
                fileChannel.close();
                offsetFile.close();
            } catch (IOException e) {
                LOGGER.warn("close offset file ({}) failed.", offsetFile);
            }
        }
    }

}
