package com.ye.mydb.backend.dm.page;

import com.ye.mydb.backend.dm.pageCache.PageCache;
import com.ye.mydb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * DB启动时给100-107字节处填入一个随机字节，db关闭时将其拷贝到108-115字节
 * 用来做启动检查，判断上一次数据库是否正常关闭
 * 数据库每次启动的时候，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;
    
    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    private static void setVcOpen(byte[] raw) {
        //随机在LEN_VC中放字节，在100 - 107中写
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcOpen(Page pg){
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    public static void setVcClose(Page pg){
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw){
        //在108 - 115中写
        System.arraycopy(raw,OF_VC,raw,OF_VC + LEN_VC,LEN_VC);
    }

    private static boolean checkVc(byte[] raw){
        //判断100 - 107 与 108 - 115是否相同
        return Arrays.equals(Arrays.copyOfRange(raw,OF_VC,OF_VC + LEN_VC),Arrays.copyOfRange(raw,OF_VC + LEN_VC,OF_VC + 2 * LEN_VC));
    }

    public static boolean checkVc(Page pg){
        return checkVc(pg.getData());
    }
}
