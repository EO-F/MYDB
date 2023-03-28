package com.ye.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import com.ye.mydb.backend.utils.Panic;
import com.ye.mydb.backend.utils.Parser;
import com.ye.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志的二进制文件按照如下格式进行排布：
 *  [XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 *  XChecksum是一个四字节的整数，是对后序所有日志计算的校验和.
 *  Log1 - LogN是常规的日志数据
 *  BadTail是在数据库崩溃时，没有来得及写完的日志数据，不一定存在
 *
 *
 *  每条日志的格式如下：
 *  [Size][Checksum][Data]
 *  Size是一个四字节整数，标识了Data段的字节数
 *  CheckSum则是该条日志的校验和 4字节int
 */
public class LoggerImpl implements Logger{

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;

    private static final int OF_CHECKSUM = OF_SIZE + 4;

    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long position;      //当前日志指针的位置
    private long fileSize;      //初始化时记录，log操作不更新
    private int xChecksum;

    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum) {
        this.file = file;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init(){
        long size = 0;
        try{
            size = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        if(size < 4){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try{
            fc.position(0);
            fc.read(raw);
        }catch (IOException e){
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    //对单条日志求校验和
    private int calChecksum(int xCheck,byte[] log){
        for(byte b : log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    //设计成迭代器模式
    private byte[] internNext(){
        if(position + OF_DATA >= fileSize){
            return null;
        }
        //读当前日志四个字节，获取当前日志的大小
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try{
            fc.position(position);
            fc.read(tmp);
        }catch (IOException e){
            Panic.panic(e);
        }
        //获得日志大小
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize){
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try{
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        byte[] log = buf.array();

        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log,OF_DATA,log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        if(checkSum1 != checkSum2){
            return null;
        }
        position += log.length;
        return log;
    }

    //校验日志文件的XChecksum，并移除尾部可能存在的BadTail
    //由于BadTail该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，去掉BadTail即可保证日志文件的一致性
    private void checkAndRemoveTail(){
        rewind();

        int xCheck = 0;
        while(true){
            byte[] log = internNext();
            if(log == null)break;
            xCheck = calChecksum(xCheck,log);
        }
        if(xCheck != xChecksum){
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rewind();
    }

    //进行日志记录
    @Override
    public void log(byte[] data) {
       byte[] log = wrapLog(data);
       ByteBuffer buf = ByteBuffer.wrap(log);
       lock.lock();
       try{
           fc.position(fc.size());
           fc.write(buf);
       }catch (IOException e){
           Panic.panic(e);
       }finally {
           lock.unlock();
       }
       updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        //加入了日志，故总校验和应该改变
        this.xChecksum = calChecksum(this.xChecksum,log);
        try{
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        //合成日志
        byte[] checkSum = Parser.int2Byte(calChecksum(0,data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size,checkSum,data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try{
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try{
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
