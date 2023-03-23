package com.ye.mydb.backend.utils;

public class Panic {
    //进行强制停机,在一些基础模块中，无法恢复的错误直接停机
    public static void panic(Exception err){
        err.printStackTrace();;
        System.exit(1);
    }
}
