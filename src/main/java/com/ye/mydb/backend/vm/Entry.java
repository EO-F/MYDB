package com.ye.mydb.backend.vm;

import com.google.common.primitives.Bytes;
import com.ye.mydb.backend.common.SubArray;
import com.ye.mydb.backend.dm.dataItem.DataItem;
import com.ye.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出
 * entry结构：
 * [XMIN][XMAX][data]
 * XMIN是创建该条记录（版本）的事务编号，而XMAX则是删除该条记录（版本）的事务编号。
 * XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm,DataItem dataItem,long uid){
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static byte[] wrapEntryRaw(long xid,byte[] data){
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin,xmax,data);
    }

    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove(){
        dataItem.release();
    }

    //以拷贝的形式返回内容
    public byte[] data(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw,sa.start + OF_DATA,data,0,data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start + OF_XMAX,sa.start + OF_DATA));
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw,sa.start + OF_XMIN,sa.start + OF_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid){
        dataItem.before();
        try{
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,sa.raw,sa.start + OF_XMAX, 8);
        }finally {
            dataItem.after(xid);
        }
    }

    public long getUid(){
        return uid;
    }
}
