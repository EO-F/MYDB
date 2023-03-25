package com.ye.mydb.backend.dm;

import com.google.common.primitives.Bytes;
import com.ye.mydb.backend.common.SubArray;
import com.ye.mydb.backend.dm.logger.Logger;
import com.ye.mydb.backend.dm.page.Page;
import com.ye.mydb.backend.dm.page.PageX;
import com.ye.mydb.backend.dm.pageCache.PageCache;
import com.ye.mydb.backend.tm.TransactionManager;
import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.backend.utils.Parser;

import java.util.*;

public class Recover {
    /**
     * DM为上层模块，提供了两种操作，分别是插入新数据（I）和更新现有数据(U).
     * 日志策略：在进行I和U操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作
     * 日志在数据操作之前，保证到达了磁盘，那么即使该数据操作最后没有来得及同步到磁盘，数据库就发生了崩溃，后续也可以通过磁盘上的日志恢复该数据。
     * (Ti, I, A, x)，表示事务 Ti 在 A 位置插入了一条数据 x
     * (Ti, U, A, oldx, newx)，表示事务 Ti 将 A 位置的数据，从 oldx 更新成 newx
     */

    /**
     * 单线程下
     * 由于单线程，Ti、Tj 和 Tk 的日志永远不会相交。日志恢复很简单，假设日志中最后一个事务是 Ti：
     * 1.对 Ti 之前所有的事务的日志，进行重做（redo）
     * 2.接着检查 Ti 的状态（XID 文件），如果 Ti 的状态是已完成（包括 committed 和 aborted），就将 Ti 重做，否则进行撤销（undo）
     *
     * 如何对事务 T 进行 redo：
     * 1.正序扫描事务 T 的所有日志
     * 2.如果日志是插入操作 (Ti, I, A, x)，就将 x 重新插入 A 位置
     * 3.如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 newx
     *
     * undo：
     * 1.倒序扫描事务 T 的所有日志
     * 2.如果日志是插入操作 (Ti, I, A, x)，就将 A 位置的数据删除
     * 3.如果日志是更新操作 (Ti, U, A, oldx, newx)，就将 A 位置的值设置为 oldx
     *
     * MYDB 中其实没有真正的删除操作，对于插入操作的 undo，只是将其中的标志位设置为 invalid。对于删除的探讨将在 VM 中进行。
     */

    /**
     * 多线程下
     * <p>
     * 情况1：
     * T1 begin
     * T2 begin
     * T2 U(x)
     * T1 R(x)
     * ...
     * T1 commit
     * MYDB break down
     * <p>
     * 在系统崩溃时，T2 仍然是活跃状态。那么当数据库重新启动，执行恢复例程时，会撤销 T2，它对数据库的影响会被消除。
     * 但是由于 T1 读取了 T2 更新的值，既然 T2 被撤销，那么 T1 也应当被撤销。这种情况，就是级联回滚。但是，T1 已经 commit 了，
     * 所有 commit 的事务的影响，应当被持久化。这里就造成了矛盾。所以这里需要保证：
     * <p>
     * ****正在进行的事务，不会读取其他任何未提交的事务产生的数据****
     * <p>
     * 情况2：
     * x初值为0
     * T1 begin
     * T2 begin
     * T1 set x = x+1 // 产生的日志为(T1, U, A, 0, 1)
     * T2 set x = x+1 // 产生的日志为(T1, U, A, 1, 2)
     * T2 commit
     * MYDB break down
     * <p>
     * 在系统崩溃时，T1 仍然是活跃状态。那么当数据库重新启动，执行恢复例程时，会对 T1 进行撤销，对 T2 进行重做，
     * 但是，无论撤销和重做的先后顺序如何，x 最后的结果，要么是 0，要么是 2，这都是错误的。
     * T1，T2都设置x为1（多线程），若先undoT1，则0-1+1 = 0 ，先redoT2，0 + 1 - 1= 0；若T2在崩溃前正确提交，则为2，都与正确结果1不同
     * 因为我们的日志太过简单, 仅仅记录了”前相”和”后相”. 并单纯的依靠”前相”undo, 依靠”后相”redo.
     * 这种简单的日志方式和恢复方式, 并不能涵盖住所有数据库操作形成的语义
     * <p>
     * 解决方法有两种：
     * 1.增加日志种类
     * 2.限制数据库操作
     * MYDB 采用的是限制数据库操作，需要保证：
     * ****正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据****
     * <p>
     * 在 MYDB 中，由于 VM 的存在，传递到 DM 层，真正执行的操作序列，都可以保证规定 1 和规定 2
     */

    // updateLog:
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]

    // insertLog:
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    //规定两种日志的格式
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 0;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgno = 0;
        while (true) {
            //恢复前将所有日志文件分类变为I或U日志
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if (pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if (maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + "pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        //判断是不是未提交，未提交的加入缓存中，进行undo
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有active log进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oidRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw,raw.start,raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oidRaw,newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        //将 log 数组中从 OF_UPDATE_UID 到 OF_UPDATE_RAW 索引位置的子数组转换为长整型数值，
        // 并将该值的低16位赋给 li.offset 属性，将剩余32位的高位赋给 li.pgno 属性。
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int) (uid & (1L << 32) - 1);
        //计算需要复制的原始数据长度 length，并将 log 数组中从 OF_UPDATE_RAW 索引位置开始的长度为 length 的子数组复制到 li.oldRaw 属性中，
        // 同时将从 OF_UPDATE_RAW + length 开始的相同长度的子数组复制到 li.newRaw 属性中。
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if (flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try{
            PageX.recoverUpdate(pg,raw,offset);
        }finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid,Page pg,byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw,xidRaw,pgnoRaw,offsetRaw,raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log){
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET);
        li.offset = Parser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc ,byte[] log,int flag){
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try{
            pg = pc.getPage(li.pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            if(flag == UNDO){
                //就是将该条 DataItem 的有效位设置为无效，来进行逻辑删除。
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg,li.raw,li.offset);
        }finally {
            pg.release();
        }
    }
}
