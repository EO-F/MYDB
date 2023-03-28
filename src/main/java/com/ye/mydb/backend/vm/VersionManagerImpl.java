package com.ye.mydb.backend.vm;

import com.ye.mydb.backend.common.AbstractCache;
import com.ye.mydb.backend.dm.DataManager;
import com.ye.mydb.backend.tm.TransactionManager;
import com.ye.mydb.backend.tm.TransactionManagerImpl;
import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.common.Error;

import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{

    /**
     * 只看更新操作（U）和读操作（R），两个操作只要满足下面三个条件，就可以说这两个操作相互冲突：
     * 1.这两个操作是由不同的事务执行的
     * 2.这两个操作操作的是同一个数据项
     * 3.这两个操作至少有一个是更新操作
     *
     * 交换两个互不冲突的操作的顺序，不会对最终的结果造成影响，而交换两个冲突操作的顺序，则是会有影响的。
     */


    /**
     * MVCC
     * DM 层向上层提供了数据项（Data Item）的概念，VM 通过管理所有的数据项，向上层提供了记录（Entry）的概念
     * 上层模块通过 VM 操作数据的最小单位，就是记录。VM 则在其内部，为每个记录，维护了多个版本（Version）。
     * 每当上层模块对某个记录进行修改时，VM 就会为这个记录创建一个新的版本。
     *
     * T1 想要更新记录 X 的值，于是 T1 需要首先获取 X 的锁，接着更新，也就是创建了一个新的 X 的版本，假设为 x3。
     * 假设 T1 还没有释放 X 的锁时，T2 想要读取 X 的值，这时候就不会阻塞，MYDB 会返回一个较老版本的 X，
     * 例如 x2。这样最后执行的结果，就等价于，T2 先执行，T1 后执行，调度序列依然是可串行化的。如果 X 没有一个更老的版本，那只能等待 T1 释放锁了。所以只是降低了概率。
     *
     * 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
     * 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
     */

    TransactionManager tm;
    DataManager dm;
    Map<Long,Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID,Transaction.newTransaction(TransactionManagerImpl.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }

        Entry entry = null;
        try{
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException){
                return null;
            }else {
                throw e;
            }
        }
        try{
            //判断对当前记录的可见性
            if(Visibility.isVisible(tm,t,entry)){
                return entry.data();
            }else{
                return null;
            }
        }finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }
        //将数据打包成记录
        byte[] raw = Entry.wrapEntryRaw(xid,data);
        return dm.insert(xid,raw);
    }

    //一是可见性判断，二是获取资源的锁，三是版本跳跃判断。删除的操作只有一个设置 XMAX
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }
        Entry entry = null;
        try{
            entry = super.get(uid);
        }catch (Exception e){
            if(e == Error.NullEntryException){
                return false;
            }else{
                throw e;
            }
        }
        try{
            if(!Visibility.isVisible(tm,t,entry)){
                return false;
            }
            Lock l = null;
            try{
                l = lt.add(xid, uid);
            }catch (Exception e){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null){
                //尝试获取锁
                l.lock();
                l.unlock();
            }
            if(entry.getXmax() == xid){
                return false;
            }
            if(Visibility.isVersionSkip(tm,t,entry)){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;
        }finally {
            entry.release();
        }
    }

    //begin() 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用
    @Override
    public long begin(int level) {
        lock.lock();
        try{
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid,level,activeTransaction);
            activeTransaction.put(xid,t);
            return xid;
        }finally {
            lock.unlock();
        }
    }

    //commit() 方法提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态：
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try{
            if(t.err != null){
                throw t.err;
            }
        }catch (NullPointerException e){
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(e);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    //abort 事务的方法则有两种，手动和自动。手动指的是调用 abort() 方法，
    // 而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
    private void internAbort(long xid, boolean b) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!t.autoAborted){
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted)return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry){
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this,uid);
        if(entry == null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
