package com.panda.zkclient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistributeLockTest {
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    countDownLatch.await();
                    DistributeLock lock = new DistributeLock();
                    lock.lock();
                    TimeUnit.SECONDS.sleep(5);
                    lock.unlock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, "thread--" + i).start();
            countDownLatch.countDown();
        }
    }
}
