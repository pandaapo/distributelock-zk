package com.panda.zkclient_onlyone_suequential;

import org.I0Itec.zkclient.IZkDataListener;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 高性能分布式锁：一个客户端创建一个节点，最终创建多个临时顺序节点
 */
public class HighPerformanceZkLock extends AbstractLock {

    private static final String PATH = "/highPerformance_zklock";
    //当前节点路径
    private String currentPath;
    //前一个节点的路径
    private String beforePath;

    private CountDownLatch countDownLatch = null;

    public HighPerformanceZkLock(){
        //如果不存在这个节点，则创建持久节点。 ？？？（通病）PATH节点不存在时，这里在多线程的情况下报错：KeeperException$NodeExistsException: KeeperErrorCode = NodeExists for /xxxxx，不try catch运行不下去。？？？
        if (!zkClient.exists(PATH)) {
            try {
                zkClient.createPersistent(PATH);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean tryLock() {
        //如果currentPath为空则第一次尝试加锁，第一次加锁赋值currentPath
        if(null == currentPath || "".equals(currentPath)){
            //在path下创建一个临时的顺序节点
            currentPath = zkClient.createEphemeralSequential(PATH + "/", "lock");
        }
        //获取所有的临时节点，并排序
        List<String> children = zkClient.getChildren(PATH);
        Collections.sort(children);
        if (currentPath.equals(PATH + "/" + children.get(0))) {
            return true;
        } else {
            //如果当前节点不是排名第一，则获取它前面的节点名称，并赋值给beforePath
            int pathLength = PATH.length();
            int wz = Collections.binarySearch(children, currentPath.substring(pathLength + 1));
            beforePath = PATH + "/" + children.get(wz - 1);
        }
        return false;
    }

    @Override
    public void releaseLock() {
        if (null != zkClient) {
            zkClient.delete(currentPath);
            zkClient.close();
        }
    }

    @Override
    public void waitLock() {
        IZkDataListener iZkDataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                if (null != countDownLatch) {
                    countDownLatch.countDown();
                }
            }
        };
        //监听前一个节点的变化
        zkClient.subscribeDataChanges(beforePath, iZkDataListener);
        if (zkClient.exists(beforePath)) {
            countDownLatch = new CountDownLatch(1);
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        zkClient.unsubscribeDataChanges(beforePath, iZkDataListener);
    }
}
