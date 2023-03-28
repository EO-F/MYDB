package com.ye.mydb.backend.tm;

import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.backend.utils.Parser;
import com.ye.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    /**
     * 在MYDB中，每个事务都有一个XID，这个ID唯一标识了这个事务。事务的XID从1开始标号，并自增，不可重复。并特殊规定
     * XID为 0 是一个超级事务，XID为0的事务的状态永远是committed，状态不需要记录
     */

    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;

    //每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    //超级事务，永远为committed状态
    public static final long SUPER_XID = 0;

    //XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;

    private FileChannel fc;

    private long xidCounter;

    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;

        try {
            fileLen = file.length();
        } catch (IOException e) {
            //进行强制停机
            Panic.panic(Error.BadXIDFileException);
        }

        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    /**
     *  事务xid在文件中的状态就存储在（xid - 1）+ 8字节处，xid - 1是因为xid 0（Super XID）的状态不需要记录，且保存了一个8字节的数字
     *  根据事务xid取得其在xid文件中对应的位置
     */
    private long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            //事务xid加一个
            long xid = xidCounter + 1;
            //改变该事务的状态
            updateXID(xid,FIELD_TRAN_ACTIVE);
            //堆XIDCounter进行增加
            incrXIDCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            //写入xid文件中
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            //将channel里面还未写入的数据全部刷新到磁盘中
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private void updateXID(long xid, byte status) {
        //获得当前事务id在xid文件中的偏移量
        long offset = getXidPosition(xid);
        //分配大小为1的字节数组
        byte[] tmp = new byte[XID_FIELD_SIZE];
        //赋值为当前的状态
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            //设置为事务的偏移量
            fc.position(offset);
            //写入xid文件中
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            //类似BIO中的flush()
            //false表示不同步文件的元数据
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //检测XID事务是否处于status状态
    private boolean checkXID(long xid,byte status){
        //获取该xid事务的偏移地址
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);

        try {
            //读取到buf中
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //判断是否与当前status相同
        return buf.array()[0] == status;
    }

    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID)
            return false;
        return checkXID(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        //超级xid只有一种状态，即已提交
        if(xid == SUPER_XID)
            return true;
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID)
            return false;
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
