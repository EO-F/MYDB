package com.ye.mydb.backend.dm;

import com.ye.mydb.backend.common.AbstractCache;
import com.ye.mydb.backend.dm.dataItem.DataItem;
import com.ye.mydb.backend.dm.dataItem.DataItemImpl;
import com.ye.mydb.backend.dm.logger.Logger;
import com.ye.mydb.backend.dm.page.Page;
import com.ye.mydb.backend.dm.page.PageOne;
import com.ye.mydb.backend.dm.page.PageX;
import com.ye.mydb.backend.dm.pageCache.PageCache;
import com.ye.mydb.backend.dm.pageIndex.PageIndex;
import com.ye.mydb.backend.dm.pageIndex.PageInfo;
import com.ye.mydb.backend.tm.TransactionManager;
import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.backend.utils.Types;
import com.ye.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pc, Logger logger) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.logger = logger;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if(!di.isValid()){
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }
        //尝试获取可用页
        PageInfo pi = null;
        for(int i = 0;i < 5;i++){
            pi = pIndex.select(raw.length);
            if(pi != null){
                break;
            }else{
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno,PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null){
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try{
            pg = pc.getPage(pi.pgno);
            //首先做日志
            byte[] log = Recover.insertLog(xid,pg,raw);
            logger.log(log);
            //再执行插入操作
            short offset = PageX.insert(pg,raw);

            pg.release();
            return Types.addressToUid(pi.pgno,offset);
        }finally {
            //将取出的pg重新插入pIndex
            if(pg != null){
                pIndex.add(pi.pgno,PageX.getFreeSpace(pg));
            }else{
                pIndex.add(pi.pgno,freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        //设置第一页的字节校验
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        //1.将long类型的uid的低16位（即最后16个比特位）提取出来，并将它作为short类型的offset值。
        short offset = (short) (uid & ((1L << 16) - 1));
        //2.将long类型的uid右移32位，以便提取剩余的32位（即前32个比特位），并将它们作为int类型的pgno值。
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));

        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg,offset,this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    //为xid生成update日志
    public void logDataItem(long xid,DataItem di){
        byte[] log = Recover.updateLog(xid,di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }
}
