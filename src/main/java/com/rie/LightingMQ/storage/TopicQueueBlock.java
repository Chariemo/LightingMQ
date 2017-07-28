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
import java.util.Timer;
import java.util.function.DoubleToIntFunction;

/**
 * frame{dataLength(int) + data}
 * Created by Charley on 2017/7/23.
 */
public class TopicQueueBlock {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueueBlock.class);
    public static final String BLOCK_FILE_SUFFIX = ".lmq_data";  //数据文件后缀
    public static final int BLOCK_SIZE = 5 * 1024;  //5kB
    public static final int EOF = -1;   // 文件结尾
    private String blockFilePath;
    private Index index;
    private RandomAccessFile blockFile;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private MappedByteBuffer mappedByteBuffer;
    private OffsetHelper offsetHelper;

    public TopicQueueBlock(String blockFilePath, Index index, OffsetHelper offsetHelper, RandomAccessFile blockFile,
                           FileChannel fileChannel, ByteBuffer byteBuffer, MappedByteBuffer mappedByteBuffer) {

        this.blockFilePath = blockFilePath;
        this.index = index;
        this.offsetHelper = offsetHelper;
        this.blockFile = blockFile;
        this.fileChannel = fileChannel;
        this.byteBuffer = byteBuffer;
        this.mappedByteBuffer = mappedByteBuffer;
    }

    public TopicQueueBlock(String blockFilePath, Index index, OffsetHelper offsetHelper) {

        this.blockFilePath = blockFilePath;
        this.index = index;
        this.offsetHelper = offsetHelper;

        try {
            File file = new File(blockFilePath);
            blockFile = new RandomAccessFile(file, "rw");
            fileChannel = blockFile.getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
            byteBuffer = mappedByteBuffer.load();

        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new IllegalArgumentException(e);
        }
    }

    public TopicQueueBlock duplicate() {

        return new TopicQueueBlock(this.blockFilePath, this.index, this.offsetHelper, this.blockFile, this.fileChannel, this.mappedByteBuffer,
                this.mappedByteBuffer);
    }

    // 获取block的文件名
    public static String formatBlockFilePath(String fileBackupDir, String queueName, int fileNo) {

        return fileBackupDir + File.separator + String.format("block_%s_%d%s", queueName, fileNo, BLOCK_FILE_SUFFIX);
    }

    public String getBlockFilePath() {

        return this.blockFilePath;
    }

    public void setEof() {

        this.byteBuffer.position(index.getWriterIndex());
        this.byteBuffer.putInt(EOF);
    }

    public boolean isWritable(int len) {

        int increment = len + Integer.BYTES;   //length + data
        return BLOCK_SIZE >= increment + index.getWriterIndex() + Integer.BYTES;
    }

    public boolean isEof() {

        int readerIndex = index.getReaderIndex();
        return readerIndex >= 0 && byteBuffer.getInt(readerIndex) == EOF;
    }

    public int write(byte[] data) {

        int len = data.length;
        int increment = len + Integer.BYTES;
        int writerIndex = index.getWriterIndex();
        System.out.println("writeFileNo: " + index.getWriteFileNo() + " writerIndex: " + writerIndex);
        byteBuffer.position(writerIndex);
        byteBuffer.putInt(len);
        byteBuffer.put(data);
        index.setWriterIndex(increment + writerIndex);
        index.setWriteCounter(index.getWriteCounter() + 1);
        // 保存当前写入数据的索引信息
        offsetHelper.write(index.getWriteCounter(), index.getWriteFileNo(), writerIndex);
        return increment;
    }

    public byte[] read() {

        byte[] result = null;
        int readFileNo = index.getReadFileNo();
        int readerIndex = index.getReaderIndex();
        int writeFileNo = index.getWriteFileNo();
        int writerIndex = index.getWriterIndex();

        if (readFileNo == writeFileNo && readerIndex >= writerIndex) {
            return null;
        }

        byteBuffer.position(readerIndex);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0) {
            return null;
        }

        result = new byte[dataLength];
        byteBuffer.get(result);
        index.setReaderIndex(readerIndex + dataLength + Integer.BYTES);
        index.setReadCounter(index.getReadCounter() + 1);
        return result;
    }

    public byte[] read(int readerIndex) {

        byte[] result = null;
        byteBuffer.position(readerIndex);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0) {
            return null;
        }
        result = new byte[dataLength];
        byteBuffer.get(result);
        return result;
    }


    public void sync() {

        if (mappedByteBuffer != null) {
            mappedByteBuffer.force();
        }
    }

    public void close() {

        if (blockFile == null) {
            return;
        }
        sync();
        try {
            Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            method.setAccessible(true);
            method.invoke(FileChannelImpl.class, mappedByteBuffer);
        } catch (Exception e) {
            LOGGER.warn("close queue block file failed.");
        }
        mappedByteBuffer = null;
        byteBuffer = null;
        try {
            fileChannel.close();
            blockFile.close();
        } catch (IOException e) {
            LOGGER.warn("close queue block file failed.");
        }
    }
}
