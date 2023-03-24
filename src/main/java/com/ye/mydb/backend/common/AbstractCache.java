package com.ye.mydb.backend.common;

import com.ye.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * @param <T>
 */
public abstract class AbstractCache<T> {

    private HashMap<Long,T> cache;      //实际缓存的数据

    private HashMap<Long,Integer> references;       //元素的引用个数

    private HashMap<Long,Boolean> getting;      //正在获取资源的线程

    private int maxResource;        //缓存的最大缓存资源数
    private int count = 0;      //缓存中的资源个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception{
        while (true){
            lock.lock();
            if(getting.containsKey(key)){
                //请求的资源正在被被其他线程获取
                lock.unlock();

                try{
                    Thread.sleep(1);
                }catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)){
                //资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key,references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            //尝试获取资源,该资源不在缓存之中
            if(maxResource > 0 && count == maxResource){
                //判断当前缓存已满，抛出异常
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            //将当前线程设置为正在获取key资源，推出while循环进行获取
            getting.put(key,true);
            lock.unlock();
            break;
        }

        T obj = null;
        try{
            obj = getForCache(key);
        }catch (Exception e){
            lock.lock();
            //获取不在缓存的资源失败
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //获取不在缓存的资源成功
        lock.lock();
        getting.remove(key);
        //将资源放入缓存之中
        cache.put(key,obj);
        references.put(key,1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key){
        lock.unlock();
        try{
            //获取当前资源的引用计数，减去当前线程
            int ref = references.get(key) - 1;
            if(ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }else{
                //引用不为零，仅仅为当前线程进行释放引用
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close(){
        lock.unlock();
        try{
            Set<Long> keys = cache.keySet();
            for(long key : keys){
                //对所有缓存进行释放
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     *
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被去驱逐时的写回行为
     * @param obj
     */
    protected abstract void releaseForCache(T obj);
}
