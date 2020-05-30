package com.panda.zkclient_onlyone_suequential;

public class ZkLockTest {
    public static void main(String[] args) {
        //模拟10个客户端，一个客户端一个线程
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(new LockRunnable());
            thread.start();
        }
    }

    static class LockRunnable implements Runnable {
        @Override
        public void run() {
            // 这样初始化，父类的属性也会根据对属性的默认定义进行初始化。
//            AbstractLock zkLock = new SimpleZkLock();
            AbstractLock zkLock = new HighPerformanceZkLock();
            zkLock.getLock();
            //模拟业务操作
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            zkLock.releaseLock();
        }
    }
}
