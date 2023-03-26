package com.ye.mydb.backend.dm.pageIndex;

import com.ye.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引
 * 缓存了每一页的空闲空间，用于在上一层模块进行插入操作时，能够快速找到一个合适空间的页面，而无序从磁盘或者缓存中检查每一个页面的信息
 */
public class PageIndex {
    //将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists;
    private Lock lock;

    @SuppressWarnings("unchecked")
    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0;i < INTERVALS_NO + 1;i++){
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno,int freeSpace){
        lock.lock();
        try{
            int number = freeSpace / THRESHOLD;
            //上层模块使用完这个页面后，需要将其重新插入PageIndex
            //给空闲分页添加信息
            lists[number].add(new PageInfo(pgno,freeSpace));
        }finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize){
        lock.lock();
        try{
            int number = spaceSize / THRESHOLD;
            //这是因为在计算需要查找的数据所占页面数量时，可能会存在一个精度问题，导致计算结果比实际需要的页面数量少1。
            // 因此，在number小于预设的区间数量时，为避免漏掉最后一页的数据，需要将number自增1。
            //举个例子，假设THRESHOLD为10，spaceSize为99，那么计算出的number为9（99/10=9.9），
            // 但实际上需要10页才能存储全部数据。因此，在这种情况下，需要将number自增1，才能保证访问到所有数据。
            if(number < INTERVALS_NO) number++;
            while(number <= INTERVALS_NO){
                if(lists[number].size() == 0){
                    number++;
                    continue;
                }
                //如果number小于等于预先设定的区间数量INTERVALS_NO，则在这些区间中选择一个非空的页面列表并返回其中的第一个页面（从列表中移除并返回）
                //被选择的页，会直接从PageIndex中移除，意味着，同一个页面是不允许并发写的
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }
}
