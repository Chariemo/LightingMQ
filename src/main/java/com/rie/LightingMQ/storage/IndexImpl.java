package com.rie.LightingMQ.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by Charley on 2017/7/25.
 */
public class IndexImpl implements Index {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexImpl.class);
    public static final String INDEX_FILE_SUFFIX = ".lmq_index";
    private volatile int readFileNo;    // 8    读索引文件号
    private volatile int readerIndex;   // 12   读索引位置
    private volatile int readCounter;   // 16   总读取数量
    private volatile int writeFileNo;   // 20   写索引文件号
    private volatile int writerIndex;   // 24   写索引位置
    private volatile int writeCounter;  // 28   总写入数量

    private RandomAccessFile indexFile;
    private FileChannel indexFileChannel;
    private MappedByteBuffer index;

    public IndexImpl(String queueName, String fileDir) {

        String indexFilePath = formatIndexFilePath(queueName, fileDir);
        System.out.println("index file path: " + indexFilePath);
        File file = new File(indexFilePath);
        try {

            if (file.exists()) {
                this.indexFile = new RandomAccessFile(file, "rw");
                byte[] magic = new byte[8];
                this.indexFile.read(magic, 0, 8);
                if (!MAGIC.equals(new String(magic))) {
                    throw new IllegalArgumentException("magic version mismatch");
                }
                this.readFileNo = indexFile.readInt();
                this.readerIndex = indexFile.readInt();
                this.readCounter = indexFile.readInt();
                this.writeFileNo = indexFile.readInt();
                this.writerIndex = indexFile.readInt();
                this.writeCounter = indexFile.readInt();
                this.indexFileChannel = indexFile.getChannel();
                this.index = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.index = index.load();
            }
            else {
                this.indexFile = new RandomAccessFile(file, "rw");
                this.indexFileChannel = indexFile.getChannel();
                this.index = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                setMagic();
                setReadFileNo(1);
                setReaderIndex(0);
                setReadCounter(0);
                setWriteFileNo(1);
                setWriterIndex(0);
                setWriteCounter(0);
            }
        } catch (IOException e) {
            LOGGER.error("something wrong while create IndexImpl.");
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public int getReadFileNo() {
        return this.readFileNo;
    }

    @Override
    public int getReaderIndex() {
        return this.readerIndex;
    }

    @Override
    public int getReadCounter() {
        return this.readCounter;
    }

    @Override
    public int getWriteFileNo() {
        return this.writeFileNo;
    }

    @Override
    public int getWriterIndex() {
        return this.writerIndex;
    }

    @Override
    public int getWriteCounter() {
        return this.writeCounter;
    }

    @Override
    public void setMagic() {

        this.index.position(0);
        this.index.put(MAGIC.getBytes());
    }

    @Override
    public void setReadFileNo(int readFileNo) {

        this.index.position(READ_FILENO_OFFSET);
        this.index.putInt(readFileNo);
        this.readFileNo = readFileNo;
    }

    @Override
    public void setReaderIndex(int readerIndex) {

        this.index.position(READERINDEX_OFFSET);
        this.index.putInt(readerIndex);
        this.readerIndex = readerIndex;
    }

    @Override
    public void setReadCounter(int readCounter) {

        this.index.position(READ_CNT_OFFSET);
        this.index.putInt(readCounter);
        this.readCounter = readCounter;
    }

    @Override
    public void setWriteFileNo(int writeFileNo) {

        this.index.position(WRITE_FILENO_OFFSET);
        this.index.putInt(writeFileNo);
        this.writeFileNo = writeFileNo;
    }

    @Override
    public void setWriterIndex(int writerIndex) {

        this.index.position(WRITERINDEX_OFFSET);
        this.index.putInt(writerIndex);
        this.writerIndex = writerIndex;
    }

    @Override
    public void setWriteCounter(int writeCounter) {

        this.index.position(WRITE_CNT_OFFSET);
        this.index.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }

    @Override
    public void sync() {

        if (index != null) {
            index.force();
        }
    }

    @Override
    public void close() {

        if (indexFile != null) {
            sync();
            try {
                Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
                method.setAccessible(true);
                method.invoke(FileChannelImpl.class, index);
            } catch (Exception e) {
                LOGGER.warn("close index file ({}) failed.", indexFile);
            }
            index = null;
            try {
                indexFileChannel.close();
                indexFile.close();
            } catch (IOException e) {
                LOGGER.warn("close index file ({}) failed.", indexFile);
            }
        }
    }

    @Override
    public void reset() {

        int size = writeCounter - readCounter;
        setReadCounter(0);
        setWriteCounter(size);
    }

    public static String formatIndexFilePath(String queueName, String fileDir) {

        String indexFilePath = null;
        indexFilePath = fileDir + File.separator + String.format("index_%s%s", queueName, INDEX_FILE_SUFFIX);
        return indexFilePath;
    }
}
