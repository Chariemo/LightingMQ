package com.rie.LightingMQ.storage;

public interface Index {

    String MAGIC = "lmqv.1.0";
    int INDEX_SIZE = 32;            // index文件大小
    int READ_FILENO_OFFSET = 8;     // readFileNo位置
    int READERINDEX_OFFSET = 12;    // readerIndex位置
    int READ_CNT_OFFSET = 16;       // readCounter位置
    int WRITE_FILENO_OFFSET = 20;   // writeFileNo位置
    int WRITERINDEX_OFFSET = 24;    // writerIndex位置
    int WRITE_CNT_OFFSET = 28;      // writeCounter位置

    int getReadFileNo();

    int getReaderIndex();

    int getReadCounter();

    int getWriteFileNo();

    int getWriterIndex();

    int getWriteCounter();

    void setMagic();

    void setReadFileNo(int readFileNo);

    void setReaderIndex(int readerIndex);

    void setReadCounter(int readCounter);

    void setWriteFileNo(int writeFileNo);

    void setWriterIndex(int writerIndex);

    void setWriteCounter(int writeCounter);

    void sync();

    void close();

    void reset();
}
