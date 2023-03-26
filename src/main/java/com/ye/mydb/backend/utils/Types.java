package com.ye.mydb.backend.utils;

public class Types {
    public static long addressToUid(int pgno, short offset) {
        //它将给定的页号（pgno）和偏移量（offset）转换为唯一的64位长整型UID。该方法首先将页号转换为32位整数（u0），
        // 然后将偏移量转换为32位整数（u1）。最后，它将这两个整数合并为一个64位长整型UID，并返回结果。
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}