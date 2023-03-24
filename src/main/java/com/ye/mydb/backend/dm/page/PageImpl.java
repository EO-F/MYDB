package com.ye.mydb.backend.dm.page;

import com.ye.mydb.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{

    private int pageNumber;     //这个页面的页号
    private byte[] data;        //这个页实际包含的数据
    private boolean dirty;      //标志这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘
    private Lock lock;
    private PageCache pc;       //pageCache引用，用来方便拿到Page的引用时可以快速对这个页面的缓存进行释放操作


    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.lock = new ReentrantLock();
        this.pc = pc;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
