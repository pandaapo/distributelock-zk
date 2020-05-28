package com.panda.zkclient;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 使用ZkClient实现分布式锁。锁是基于Lock接口
 */
public class DistributeLock implements Lock {

    private static ZkClient client;

    private String current_path;

    private String wait_path;

    private String root_path = "/lock";

    public DistributeLock() {
        if (!client.exists(root_path)) {
            client.createPersistent(root_path);
        }
    }

    static {
        client = new ZkClient("192.168.25.128:2181,192.168.25.129:2181,192.168.25.130:2181");
    }

    @Override
    public void lock() {
        if (tryLock()) {
            System.out.println(Thread.currentThread().getName() + "->" + current_path + "获得锁成功");
            return;
        }
        try {
            waitForLock(wait_path);
            System.out.println(Thread.currentThread().getName() + "->" + current_path + "获得锁成功");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitForLock(String prev) throws InterruptedException {
        if (client.exists(prev)) {
            System.out.println(Thread.currentThread().getName() + "等待的节点是：" + prev);
            CountDownLatch latch = new CountDownLatch(1);
            client.subscribeDataChanges(prev, new IZkDataListener() {
                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    latch.countDown();
                }

                @Override
                public void handleDataDeleted(String dataPath) throws Exception {
//                    latch.countDown();
                }
            });
            latch.await();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        current_path = client.createEphemeralSequential(root_path + "/", "1");
        System.out.println(Thread.currentThread().getName() + "->创建节点：" + current_path);
        List<String> children = client.getChildren(root_path);
        SortedSet<String> sortedSet = new TreeSet<>();
        for (String str : children) {
            sortedSet.add(str);
        }
        String first = sortedSet.first();
        if (first.equals(current_path.replace(root_path + "/", ""))) {
            return true;
        }
        SortedSet<String> headSet = sortedSet.headSet(current_path.replace(root_path + "/", ""));
        if (!headSet.isEmpty()) {
            wait_path = root_path + "/" + headSet.last();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        client.writeData(current_path, "hello");
    }

    @Override
    public Condition newCondition() {
        return this.newCondition();
    }
}
