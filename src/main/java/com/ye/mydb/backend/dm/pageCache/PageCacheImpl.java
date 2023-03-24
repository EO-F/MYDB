package com.ye.mydb.backend.dm.pageCache;

import com.ye.mydb.backend.common.AbstractCache;
import com.ye.mydb.backend.dm.page.Page;
import com.ye.mydb.backend.dm.page.PageImpl;
import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;         //用于记录当前打开的数据库文件有多少页

    PageCacheImpl(RandomAccessFile file, FileChannel fc,int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try{
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        //当前存储文件长度除以页面大小即可知道页码是多少
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        //pageCache 引用设置为null
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);      //新创建的页面需要立刻被写入外存之中
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {

        //调用缓存策略的get方法
        return get((long) pgno);
    }

    @Override
    public void close() {
        //对缓存进行关闭
        super.close();
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    /**
     * 根据pageNumber页号从数据库文件中读取页数据，并包裹成Page
     *
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno,buf.array(),this);
    }

    private static long pageOffset(int pgno) {
        //页号从1开始,若要找2号页的偏移量，则，寻找一号页的结尾 ，即 1 * pagesize
        return (pgno - 1) * PAGE_SIZE;
    }

    @Override
    protected void releaseForCache(Page obj) {
        //只需判断当前页面是否为脏页面、
        //当前页面在内存之中，若页面不为脏，则当前数据未修改过，与外存中的文件数据一致，不需要进行操作
        //若页面为脏，则需要对内存中的页重新写到外存（即.db文件之中）
        if(obj.isDirty()){
            flush(obj);
            obj.setDirty(false);
        }
    }

    private void flush(Page pg) {
        //获取当前页号对应与.db文件的偏移量
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try{
            //获取当前页面在内存中的数据
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            //写入到外存之中
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    @Override
    public void release(Page page) {
        //调用缓存策略中的release方法
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try{
            file.setLength(size);
        }catch (IOException e){
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
