package com.rie.LightingMQ.storage;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by Charley on 2017/7/23.
 */
public class TopicQueue extends AbstractQueue<byte[]>{

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueue.class);
    private String fileDir;
    private Index index;
    private String queueName;
    private TopicQueueBlock readBlock;
    private TopicQueueBlock writeBlock;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private AtomicInteger size;

    public TopicQueue(String queueName, String fileDir, boolean backup) {

        this.queueName = queueName;
        this.fileDir = fileDir;
        this.index = new IndexImpl(queueName, fileDir);
        System.out.println("readFileNo: " + index.getReadFileNo() + " writeFileNo: " + index.getWriteFileNo());
        System.out.println("readerIndex: "+ index.getReaderIndex() + " writerIndex: " + index.getWriterIndex());
        this.size = new AtomicInteger(index.getWriteCounter() - index.getReadCounter());
        this.writeBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(fileDir, queueName,
                index.getWriteFileNo()), index);
        if (index.getReadFileNo() == index.getWriteFileNo()) {
            this.readBlock = this.writeBlock.duplicate();
        }
        else {
            this.readBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(fileDir,
                    queueName, index.getReadFileNo()), index);
        }
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return this.size.get();
    }

    public void rotateNextWriteBlock() {

        int nextWriteBlockFileNo = index.getWriteFileNo() + 1;
        writeBlock.setEof();
        if (index.getReadFileNo() == index.getWriteFileNo()) {
            writeBlock.sync();
        }
        else {
            writeBlock.close();
        }
        writeBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(fileDir, queueName, nextWriteBlockFileNo),
                 index);

        index.setWriteFileNo(nextWriteBlockFileNo);
        index.setWriterIndex(0);
    }

    @Override
    public boolean offer(byte[] data) {

        if (ArrayUtils.isEmpty(data)) {
            return true;
        }
        writeLock.lock();
        try {
            if (!writeBlock.isWritable(data.length)) {
                rotateNextWriteBlock();
            }
            writeBlock.write(data);
            size.incrementAndGet();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public void rotateNextReadBlock() {

        if (index.getReadFileNo() == index.getWriteFileNo()) {
            return;
        }
        readBlock.close();
        int nextReadBlockFileNo = index.getReadFileNo() + 1;
        String oldBlockFilePath = readBlock.getBlockFilePath();
        if (nextReadBlockFileNo == index.getWriteFileNo()) {
            readBlock = writeBlock.duplicate();
        }
        else {
            readBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(fileDir, queueName,
                    nextReadBlockFileNo), index);
        }

        index.setReadFileNo(nextReadBlockFileNo);
        index.setReaderIndex(0);
        TopicQueuePool.toClear(oldBlockFilePath);
    }


    @Override
    public byte[] poll() {

        byte[] result = null;
        readLock.lock();
        try {
            if (readBlock.isEof()) {
                rotateNextReadBlock();
            }
            result = readBlock.read();
            if (result != null) {
                size.incrementAndGet();
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] peek() {

        throw new UnsupportedOperationException();
    }

    public Index getIndex() {
        return index;
    }

    public void sync() {

        try {
            index.sync();
        } catch (Exception e) {
            LOGGER.error("sync error", e);
        }
        writeBlock.sync();
    }

    public void close() {

        writeBlock.close();
        if (index.getReadFileNo() != index.getWriteFileNo()) {
            readBlock.close();
        }
        index.reset();
        index.close();
    }
}
