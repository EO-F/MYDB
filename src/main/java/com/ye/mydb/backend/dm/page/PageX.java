package com.ye.mydb.backend.dm.page;

import com.ye.mydb.backend.dm.pageCache.PageCache;
import com.ye.mydb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * 一个页面以一个2字节无符号数起始，表示这一页的空闲位置的偏移。
 * 剩下的部分都是实际存储的数据
 * 对普通页的管理，基本都是围绕着对 FSO（Free Space Offset）
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE- OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    //获取pg的FSO
    public static short getFSO(Page pg){
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw){
        //byte的0-2中存储着该页的偏移量
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    //将raw插入pg中，返回插入位置
    public static short insert(Page pg,byte[] raw){
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        //在偏移量 到 raw长度 这个区间内插入数据
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        //更新偏移量
        setFSO(pg.getData(),(short) (offset + raw.length));
        return offset;
    }

    //获取空闲空间的大小
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * recoverInsert  recoverUpdate  用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。
     */

    //将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length){
            setFSO(pg.getData(),(short) (offset + raw.length));
        }
    }

    //将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }
}
