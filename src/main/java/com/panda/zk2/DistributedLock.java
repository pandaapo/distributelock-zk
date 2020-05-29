package com.panda.zk2;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 一个客户端一个线程。多个客户端，即多个new ZooKeeper()
 */
public class DistributedLock implements Lock, Watcher {
    /**
     * zk客户端
     */
    private ZooKeeper zk;

    //当前节点
    private String CURRENT_LOCK;

    //当前节点等待的前一个节点
    private String WAIT_LOCK;

    private CountDownLatch countDownLatch;

    //根节点
    private String ROOT_LOCK = "/lock";

    public DistributedLock() {
        try {
            zk = new ZooKeeper("192.168.25.131:2181,192.168.25.133:2181,192.168.25.134:2181", 10000, this);
            // while()防止未连接成功，就执行下面的zk.exists()报错。
            while (true) {
                if (ZooKeeper.States.CONNECTED == zk.getState()) {
                    break;
                }
            }
            //判断根节点是否存在
            Stat stat = zk.exists(ROOT_LOCK, false);
            if (null == stat) {
                // 创建根节点为永久
                zk.create(ROOT_LOCK, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if (this.tryLock()) {
            //表示获取锁成功
            System.out.println(Thread.currentThread().getName() + "->" + CURRENT_LOCK + "获取锁成功");
            return;
        }
        //等待获取锁
        try {
            if (StringUtils.isNotEmpty(WAIT_LOCK)) {
                waitForLock(WAIT_LOCK);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //监听比自己小的上一个节点状态，如果上一个节点删除关闭会话了，则触发监控当前节点获取锁
    private void waitForLock(String waitLock) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(waitLock, true);
        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "->" +CURRENT_LOCK + "等待" + waitLock + "释放锁");
            //当前节点的上一个节点还存在
            countDownLatch = new CountDownLatch(1);
            //等待
            countDownLatch.await();
            System.out.println(Thread.currentThread().getName() +"->" +CURRENT_LOCK + "获取锁成功");
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        try {
            // 创建临时有序节点
            CURRENT_LOCK = zk.create(ROOT_LOCK + "/", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName() + "->创建节点" + CURRENT_LOCK + "成功！");
            // 获取所有节点
            List<String> children = zk.getChildren(ROOT_LOCK, false);
            //创建一个有序的set
            SortedSet<String> sortedSet = new TreeSet<>();
            for (String child : children) {
                sortedSet.add(ROOT_LOCK + "/" + child);
            }
            // 获取最小的节点
            String firstNode = sortedSet.first();
            //如果最小的节点等于当前节点，表示获取到了锁，并且不需要监控上一个节点
            if (CURRENT_LOCK.equals(firstNode)) {
                return true;
            }

            //获取比当前节点小的节点数据，不包含当前节点
            SortedSet<String> lessThenMe = sortedSet.headSet(CURRENT_LOCK);
            //如果有比自己小的节点
            if (!lessThenMe.isEmpty()) {
                //获取比当前节点次小的接点
                WAIT_LOCK = lessThenMe.last();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        System.out.println(Thread.currentThread().getName() + ":" +CURRENT_LOCK+ "->释放锁" );
        try {
            zk.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    /**
     * implements Watcher
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        // 如果上一个节点删除，触发watch
        if(countDownLatch != null){
            countDownLatch.countDown();
        }
    }
}
