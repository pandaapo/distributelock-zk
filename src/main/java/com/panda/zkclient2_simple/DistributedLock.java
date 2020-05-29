package com.panda.zkclient2_simple;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {
    /**
     * 获取锁，如果没有得到锁就一直等待
     */
    public void qcquire() throws Exception;

    /**
     * 获取锁，如果没有得到锁就一直等待直到超时
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception;

    /**
     * 释放锁
     */
    public void release() throws Exception;
}
