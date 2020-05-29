package com.panda.zkclient2;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 一个客户端，一个线程。建立多个客户端，多个new MyZkClient()。
 */
public class SimpleDistributedLockImpl  extends BaseDistributedLock implements DistributedLock {

    /** /locker节点下所有子节点的 名称前缀(注:因为是有序的，实际上创建后的名字可能为lock-0000000001) */
    private static final String LOCK_NODE_NAME_PREFIX = "lock-";

    /** /locker节点路径 */
    private final String lockerNodePath;

    /** 当前客户端在/locker节点下创建子节点后，得到的(最终的)节点路径 */
    private String finalCurrentNodePath;

    /**
     * 构造器
     *
     * @param client
     *            zkclient客户端
     * @param lockerNodePath
     *            /locker节点路径
     */
    public SimpleDistributedLockImpl(MyZkClient client, String lockerNodePath) {
        super(client, lockerNodePath, LOCK_NODE_NAME_PREFIX);
        this.lockerNodePath = lockerNodePath;
    }


    /**
     * 获取锁的公共方法
     *
     * 注:当 time != -1 && unit != null时，才会最多只等待到指定时长，否者会一直等待下去
     * @param time
     *            等待时长
     * @param unit
     *            等待时长的单位
     * @return 是否获取到了锁
     * @throws Exception
     */
    private boolean internalLock(long time, TimeUnit unit) throws Exception {
        finalCurrentNodePath = attemptLock(time, unit);
        return finalCurrentNodePath != null;
    }

    /**
     * 一直等待---直到获取锁
     */
    public void acquire() throws Exception {
        if (!internalLock(-1, null)) {
            throw new IOException("连接丢失!在路径:'" + lockerNodePath + "'下不能获取锁!");
        }
    }

    /**
     * 最多等待指定时长---获取锁
     *
     * @return 是否获取到了锁
     * @throws Exception
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception {
        return internalLock(time, unit);
    }

    /**
     * 释放锁
     *
     * @throws Exception
     */
    public void release() throws Exception {
        releaseLock(finalCurrentNodePath);
    }

}
