package com.panda.zkclient_onlyone_suequential;

import org.I0Itec.zkclient.IZkDataListener;

import java.util.concurrent.CountDownLatch;

/**
 * 简单分布式锁：每次所有客户端同时竞争锁
 */
public class SimpleZkLock extends AbstractLock {

    private static final String NODE_NAME = "/test_simple_lock";

    private CountDownLatch countDownLatch;

    //直接创建临时节点，如果创建成功，则表示获取了锁，创建不成功则处理异常
    @Override
    public boolean tryLock() {
        if (null == zkClient) return false;
        // 多个线程/客户端并发地创建NODE_NAME，只会有一个创建成功，其他的进入catch中。？？？这时候需要处理并发问题吗，还是zookeeper的节点创建天生具备互斥性而无需处理并发问题？？？
        try {
            zkClient.createEphemeral(NODE_NAME);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void releaseLock() {
        if (null != zkClient) {
            //删除节点
            zkClient.delete(NODE_NAME);
            zkClient.close();
            System.out.println(Thread.currentThread().getName()+"释放锁成功");
        }
    }

    @Override
    public void waitLock() {
        //监听器
        IZkDataListener iZkDataListener = new IZkDataListener() {
            //节点改变被回调
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            //节点被删除回调
            @Override
            public void handleDataDeleted(String s) throws Exception {
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        };
        zkClient.subscribeDataChanges(NODE_NAME, iZkDataListener);
        //如果存在则阻塞
        if (zkClient.exists(NODE_NAME)) {
            countDownLatch = new CountDownLatch(1);
            try {
                System.out.println(Thread.currentThread().getName() + "等待获取锁...");
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //删除监听
        zkClient.unsubscribeDataChanges(NODE_NAME, iZkDataListener);
    }
}
