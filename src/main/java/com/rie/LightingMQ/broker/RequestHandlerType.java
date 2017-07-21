package com.rie.LightingMQ.broker;

/**
 * Created by Charley on 2017/7/18.
 */
public enum RequestHandlerType {

    FETCH((short)0),
    PUBLISH((short)1),
    REPLICA((short)2),
    PRE_PUBLISH((short)3);

    public final short value;

    RequestHandlerType(short i) {
        this.value  = i;
    }

    final static int size = values().length;

    public static RequestHandlerType valueOf(int index) {

        RequestHandlerType result = null;
        if (index >= 0 || index < size) {
            result = values()[index];
        }
        return result;
    }
}
