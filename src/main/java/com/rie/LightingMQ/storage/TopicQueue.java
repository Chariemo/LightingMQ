package com.rie.LightingMQ.storage;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private Index index;    //读写索引
    private String queueName;   //队列名称
    private TopicQueueBlock readBlock;  //读字块
    private TopicQueueBlock offsetReadBlock;    //offset读字块
    private int offsetReadFileNo;
    private TopicQueueBlock writeBlock; //写字块
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private OffsetHelper offsetHelper;
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private AtomicInteger size;

    public TopicQueue(String queueName, String fileDir) {

        this.queueName = queueName;
        this.fileDir = String.format("%s/%s", fileDir, queueName);
        File tempFile = new File(this.fileDir);
        if (!tempFile.exists()) {
            tempFile.mkdir();
        }
        this.fileDir = tempFile.getAbsolutePath();

        // 初始化offset读Helper
        this.offsetHelper = new OffsetHelper(queueName, this.fileDir);
        // 初始化索引
        this.index = new IndexImpl(queueName, fileDir);
        System.out.println("readFileNo: " + index.getReadFileNo() + " writeFileNo: " + index.getWriteFileNo());
        System.out.println("readerIndex: "+ index.getReaderIndex() + " writerIndex: " + index.getWriterIndex());
        // 队列大小
        this.size = new AtomicInteger(index.getWriteCounter() - index.getReadCounter());
        // 写字块初始化
        this.writeBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(this.fileDir, queueName,
                index.getWriteFileNo()), index, this.offsetHelper);
        if (index.getReadFileNo() == index.getWriteFileNo()) {
            // 读写字块为一个文件 直接复制写字块
            this.readBlock = this.writeBlock.duplicate();
        }
        else {
            this.readBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(this.fileDir,
                    queueName, index.getReadFileNo()), index, this.offsetHelper);
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

    // 转化下一写字块
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
                 index, this.offsetHelper);

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

    // 转化下一读字块
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
                    nextReadBlockFileNo), index, this.offsetHelper);
        }

        index.setReadFileNo(nextReadBlockFileNo);
        index.setReaderIndex(0);
        TopicQueuePool.toClear(oldBlockFilePath); //旧读字块加入预删除队列
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

    public byte[] offsetRead(int readCounter) {

        OffsetPOJO offsetPOJO = offsetHelper.read(readCounter); //根据readCounter获取数据的索引信息
        System.out.println("offsetPOJO" + offsetPOJO);
        byte[] result = null;

        if (null != offsetPOJO) {
            if (null == this.offsetReadBlock) { //初始化offsetReadBLock
                this.offsetReadBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(
                        fileDir, queueName, offsetPOJO.getFileNo()
                ), null, this.offsetHelper);
                this.offsetReadFileNo = offsetPOJO.getFileNo();
            }
            else if (this.offsetReadFileNo != offsetPOJO.getFileNo()) {  //判断当前offsetReadBlock是否与readCounter对应
                this.offsetReadBlock.close();
                this.offsetReadFileNo = offsetPOJO.getFileNo();
                this.offsetReadBlock = new TopicQueueBlock(TopicQueueBlock.formatBlockFilePath(
                        fileDir, queueName, this.offsetReadFileNo
                ), null, this.offsetHelper);
            }

            readLock.lock();
            try {
                result = offsetReadBlock.read(offsetPOJO.getOffset());
                return result;
            } finally {
                readLock.unlock();
            }
        }

        return null;
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
        offsetReadBlock.close();
        offsetHelper.close();
        index.reset();
        index.close();
    }
}
