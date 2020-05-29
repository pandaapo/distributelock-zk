package com.panda.zkclient2_simple;

import org.I0Itec.zkclient.ZkClient;

//？？？测试类写得不对，没用多线程，没有并发性？？？
public class DistributedLockTest {
    public static void main(String[] args) throws Exception {
        ZkClient zkClient = new ZkClient("192.168.25.131:2181,192.168.25.133:2181,192.168.25.134:2181",10000);
        SimpleDistributedLock simple = new SimpleDistributedLock(zkClient, "/locker");
        for (int i = 0; i < 100; i++) {
            try {
                simple.acquire();
                System.out.println("正在进行运算操作：" + System.currentTimeMillis());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                simple.release();
                System.out.println("========\r\n");
            }
        }
    }
}
