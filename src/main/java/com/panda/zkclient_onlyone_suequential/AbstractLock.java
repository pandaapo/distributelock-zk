package com.panda.zkclient_onlyone_suequential;

import org.I0Itec.zkclient.ZkClient;

public abstract class AbstractLock {
    //zk地址和端口
    public static final String ZK_ADDR = "192.168.205.128:2181,192.168.205.129:2181,192.168.205.130:2181";
    //超时时间
    public static final int SESSION_TIMEOUT = 10000;
    //创建zk
    protected ZkClient zkClient = new ZkClient(ZK_ADDR, SESSION_TIMEOUT);

    /**
     * 可以认为是模板模式，两个子类分别实现它的抽象方法
     * 1、简单的分布式锁：每次竞争锁的时候所有客户端都参与，zk中只有一个对应的节点。
     * 2、高性能分布式锁：每个客户端按顺序来获取锁，zk中有很多顺序节点。
     */
    public void getLock() {
        String threadName = Thread.currentThread().getName();
        if (tryLock()){
            System.out.println(threadName+"获得锁成功");
        } else {
            System.out.println(threadName+"获得锁失败，进入等待...");
            waitLock();
            //递归重新获取锁
            //1、在简单分布式锁的情形下确实需要递归，因为只有一个节点，需要不断创建删除。
            //2、？？？在高性能分布式锁的情形下还有必要递归吗？？？
            getLock();
        }
    }

    public abstract boolean tryLock();

    public abstract void releaseLock();

    public abstract void waitLock();
}
