package com.panda.zk;

import com.panda.zk.ConcurrentTest.ConcurrentTask;

public class DistributeLockTest {
    public static void main(String[] args) {
//        Runnable task1 = new Runnable(){
//            public void run() {
//                DistributeLock lock = null;
//                try {
//                    lock = new DistributeLock("192.168.25.128:2181,192.168.25.129:2181,192.168.25.130:2181","test1");
//                    //lock = new DistributedLock("127.0.0.1:2182","test2");
//                    lock.lock();
//                    Thread.sleep(3000);
//                    System.out.println("Thread " + Thread.currentThread().getName() + " running");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                finally {
//                    if(lock != null)
//                        lock.unlock();
//                }
//
//            }
//
//        };
//        new Thread(task1, "thread_0").start();

//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e1) {
//            e1.printStackTrace();
//        }

        ConcurrentTask[] tasks = new ConcurrentTask[10];
        for(int i=0;i<tasks.length;i++){
            ConcurrentTask task3 = new ConcurrentTask(){
                public void run() {
                    DistributeLock lock = null;
                    try {
                        lock = new DistributeLock("192.168.25.128:2181,192.168.25.129:2181,192.168.25.130:2181","test2");
                        lock.lock();
                        System.out.println("Thread " + Thread.currentThread().getId() + " running");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        lock.unlock();
                    }

                }
            };
            tasks[i] = task3;
        }
        new ConcurrentTest(tasks);
    }
}
