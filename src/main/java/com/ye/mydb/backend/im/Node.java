package com.ye.mydb.backend.im;

import com.ye.mydb.backend.common.SubArray;
import com.ye.mydb.backend.dm.dataItem.DataItem;
import com.ye.mydb.backend.tm.TransactionManagerImpl;
import com.ye.mydb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 *
 * LeafFlag标记了该节点是否是个叶子节点
 * KeyNumber为该节点中key的个数
 * SiblingUid是其兄弟节点存储在DM中的UID
 * 后序是穿插的子节点（SonN）和KeyN，最后的一个 KeyN 始终为 MAX_VALUE，以此方便查找。
 *
 * 持有了其 B+ 树结构的引用，DataItem 的引用和 SubArray 的引用，用于方便快速修改数据和释放数据
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    //设置/获取节点是否为叶子节点
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    //设置/获取该节点包含的key数量。
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    //设置/获取该节点的前驱/后继节点的 UID
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    //设置/获取第 k 个儿子节点的 UID。
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    //设置/获取第 k 个键值对的key。
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    //将节点从第 k 个位置开始的数据拷贝到另一个节点中
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    //将第 k 个位置之后的数据向后移动 2 个 long 的大小。
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    // 创建一个新的根节点，分别用于初始化一棵新树和创建一个空的 B+树 。
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    //从磁盘读取节点数据，并返回相应的 Node 对象。
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    //释放该节点所持有的 DataItem 资源。
    public void release() {
        dataItem.release();
    }

    //判断该节点是否为叶子节点。
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    //
    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    //在该节点中查找下一个子节点
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }
    //在该节点中查找键值对范围内的所有子节点 UID
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            //1.获取该节点的关键字数量 noKeys 和起始下标 kth，使得第 kth 个子节点对应的关键字刚好大于等于 leftKey。
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            //2.在该节点中从第 kth 个关键字开始遍历，如果某个关键字小于等于 rightKey，则将其对应的子节点添加到 uids 列表中。
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            //如果 kth 等于 noKeys，说明该节点已经被遍历完毕且没有找到符合条件的子节点，因此需要返回该节点的右兄弟节点作为搜索结果。
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            //最终，方法返回一个 LeafSearchRangeRes 对象，其中包含了符合条件的子节点列表和下一个需要搜索的兄弟节点的 uid。
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    //在该节点中插入新的键值对，并在需要时进行分裂操作
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        //获取该节点的关键字数量 noKeys 和起始下标 kth，使得第 kth 个子节点对应的关键字刚好大于等于 childKey。
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;
        //如果当前节点还没有达到最大容量，则将 newChildUid 和 childKey 插入到该节点中，然后返回 null。
        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            //否则，需要对该节点进行拆分：
            //创建一个新的叶子节点 newLeaf，并将 newChildUid 和 childKey 插入到其中；
            //将原来节点中的一半数据项移动到新的叶子节点中；
            //计算新的节点的 uid 和右兄弟节点的 uid，并更新原来节点的右兄弟节点的 uid；
            //如果该节点是根节点，则创建新的根节点；
            //返回新创建的节点和其对应的关键字。
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    //执行节点分裂操作
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
