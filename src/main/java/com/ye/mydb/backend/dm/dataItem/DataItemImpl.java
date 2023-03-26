package com.ye.mydb.backend.dm.dataItem;

import com.ye.mydb.backend.common.SubArray;
import com.ye.mydb.backend.dm.DataManagerImpl;
import com.ye.mydb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem是DM层向上层提供的数据的抽象，上层模块通过地址，向DM请求到对应的DataItem，再获取到其中的数据。
 */
public class DataItemImpl implements DataItem{

    /**
     * dataItem 结构如下：
     * [ValidFlag] [DataSize] [Data]
     * ValidFlag 1字节，0为合法，1为非法
     * DataSize  2字节，标识Data的长度
     */
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw,DataManagerImpl dm, long uid, Page pg) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        //读写锁
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.rLock = lock.readLock();
        this.wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    public boolean isValid(){
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw,raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        //把原始数据复制到一个备份数组 oldRaw 中（System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length)）。
        // 这么做是因为如果在事务执行期间出现了错误或者需要回滚事务，就需要用到这个备份数据。
        System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length);
    }

    @Override
    public void unBefore() {
        //把备份数据 oldRaw 复制回原始数据中（System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length)），撤销本次写操作；
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid,this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
