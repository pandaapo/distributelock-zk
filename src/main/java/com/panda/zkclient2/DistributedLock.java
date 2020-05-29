package com.panda.zkclient2;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁基本功能接口。没有基于juc的锁，自己实现锁的逻辑。
 */
public interface DistributedLock {
    /*
     * 获取锁，如果没有得到就等待
     */
    void acquire() throws Exception;

    /*
     * 获取锁，直到超时
     */
    boolean acquire(long time, TimeUnit unit) throws Exception;

    /*
     * 释放锁
     */
    void release() throws Exception;
}
