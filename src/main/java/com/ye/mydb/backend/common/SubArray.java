package com.ye.mydb.backend.common;

public class SubArray {
    /**
     * 在java中，两个数组无法共用一片内存，即使这两个数组的长度不同
     * 故使用SubArray类来松散的规定这个数组的可使用范围
     */
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
