package com.ye.mydb.backend.vm;

import com.ye.mydb.backend.tm.TransactionManager;

public class Visibility {

    /**
     * 再读提交下，版本对事务的可见性逻辑如下：
     * (XMIN == Ti and                             // 由Ti创建且
     *     XMAX == NULL                            // 还未被删除
     * )
     * or                                          // 或
     * (XMIN is commited and                       // 由一个已提交的事务创建且
     *     (XMAX == NULL or                        // 尚未删除或
     *     (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
     * ))
     *若条件为 true，则版本对 Ti 可见。那么获取 Ti 适合的版本，只需要从最新版本开始，依次向前检查可见性，如果为 true，就可以直接返回。
     */
    private static boolean readCommitted(TransactionManager tm,Transaction t,Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读
     *
     * 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
     * 1.在本事务后开始的事务的数据;
     * 2.本事务开始时还是 active 状态的事务的数据
     * 对于第一条，只需要比较事务 ID，即可确定。
     * 而对于第二条，则需要在事务 Ti 开始时，记录下当前活跃的所有事务 SP(Ti)，如果记录的某个版本，XMIN 在 SP(Ti) 中，也应当对 Ti 不可见。
     *
     * (XMIN == Ti and                 // 由Ti创建且
     *  (XMAX == NULL or               // 尚未被删除
     * ))
     * or                              // 或
     * (XMIN is commited and           // 由一个已提交的事务创建且
     *  XMIN < XID and                 // 这个事务小于Ti且
     *  XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
     *  (XMAX == NULL or               // 尚未被删除或
     *   (XMAX != Ti and               // 由其他事务删除但是
     *    (XMAX is not commited or     // 这个事务尚未提交或
     * XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
     * XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
     * ))))
     */

    private static boolean repeatableRead(TransactionManager tm,Transaction t,Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isVersionSkip(TransactionManager tm,Transaction t,Entry e){
        long xmax = e.getXmax();
        if(t.level == 0){
            return false;
        }else{
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    //判断可见性
    public static boolean isVisible(TransactionManager tm,Transaction t,Entry e){
        if(t.level == 0){
            return readCommitted(tm,t,e);
        }else {
            return repeatableRead(tm,t,e);
        }
    }
}
