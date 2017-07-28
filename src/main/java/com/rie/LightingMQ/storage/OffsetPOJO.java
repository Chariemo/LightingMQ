package com.rie.LightingMQ.storage;


/**
 * Created by Charley on 2017/7/28.
 */
public class OffsetPOJO {

    private int counter;
    private long timeStamp; //时间戳
    private int fileNo;
    private int offset;

    public int getCounter() {
        return counter;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public int getFileNo() {
        return fileNo;
    }

    public void setFileNo(int fileNo) {
        this.fileNo = fileNo;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {

        return "Counter: " + counter + " timeStamp: " + timeStamp +
                " fileNo: " + fileNo + " offset: " + offset;
    }
}
