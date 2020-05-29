package com.panda.zkclient2;

import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个客户端，多个线程。建立多个客户端，多个new MyZkClient()。
 * 一个客户端的多个线程公用一个节点，当该客户端所有线程都释放才释放该接点。 ？？？想不到适用场景。？？？
 */
public class DistributedLockImpl  extends BaseDistributedLock implements DistributedLock {

    /** /locker节点下所有子节点的 名称前缀(注:因为是有序的，实际上创建后的名字可能为lock-0000000001) */
    private static final String LOCK_NODE_NAME_PREFIX = "lock-";

    /** ZooKeeper客户端对应的使用锁的信息 */
    private final ConcurrentMap<ZkClient, NodeInfo> zooKeeperClientInfo = new ConcurrentHashMap<>(8);

    /** zookeeper客户端 */
    private final ZkClient zkClient;

    /** /locker节点路径 */
    private final String lockerNodePath;

    /**
     * 成员内部类 --- 用于封装 每个线程的节点数据
     *
     * 注:这里将/locker节点下的每一个子节点看作是一个lock
     *
     */
    private static class NodeInfo {

        /** 对应的节点路径 */
        final String nodePath;

        /** 该客户端内,使用该锁资源的线程数 计数器 */
        final AtomicInteger lockCount = new AtomicInteger(1);

        /** NodeInfo类的构造器 */
        private NodeInfo(String nodePath) {
            this.nodePath = nodePath;
        }
    }

    /**
     * DistributedLockMutex类的构造器
     *
     * @param client
     *            ZkClient客户端
     * @param lockerNodePath
     *            /locker节点路径
     */
    public DistributedLockImpl(MyZkClient client, String lockerNodePath) {
        super(client, lockerNodePath, LOCK_NODE_NAME_PREFIX);
        this.zkClient = client;
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
     * @return 是否获取到了锁(如果之前已经获取了锁，那么也会返回true)
     * @throws Exception
     */
    private synchronized boolean internalLock(long time, TimeUnit unit) throws Exception {
        NodeInfo nodeInfo = zooKeeperClientInfo.get(zkClient);
        if (nodeInfo != null) { // 如果此线程所在的客户端已经获取了锁
            System.out.println("zookeeper客户端" + zkClient + "已经获得了该锁了！");
            nodeInfo.lockCount.incrementAndGet();
            return true;
        }
        // 如果此线程之前未获取锁
        String nodePath = attemptLock(time, unit);
        if (nodePath != null) {
            NodeInfo newNodeInfo = new NodeInfo(nodePath);
            zooKeeperClientInfo.put(zkClient, newNodeInfo);
            return true;
        }
        return false;
    }


    /**
     * 一直等待---直到获取锁
     */
    @Override
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
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception {
        return internalLock(time, unit);
    }


    /**
     * 释放锁
     */
    @Override
    public void release(){
        NodeInfo nodeInfo = zooKeeperClientInfo.get(zkClient);
        if (nodeInfo == null) {
            throw new IllegalMonitorStateException("你不是锁: " + lockerNodePath + "的拥有者/zk客户端,无法执行此操作！");
        }
        int newLockCount = nodeInfo.lockCount.decrementAndGet();
        if (newLockCount > 0) { // 当还有其他线程在使用锁时，那么还不能释放
            return;
        }
        if (newLockCount < 0) {
            throw new IllegalMonitorStateException("锁计数器已经为负数: " + lockerNodePath);
        }
        try {
            // 只有当计数器为0时，才能正常释放锁
            releaseLock(nodeInfo.nodePath);
        } finally {
            zooKeeperClientInfo.remove(zkClient);
        }
    }

}
