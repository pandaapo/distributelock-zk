package com.panda.zkclient2_simple;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁实现
 */
public class BaseDistributedLock {
    private final ZkClient client; // Zookeeper客户端
    private final String basePath; //用于保存Zookeeper中实现分布式锁的节点，例如/locker节点，该节点是个持久节点，在该节点下下面创建临时顺序节点来实现分布式锁
    private final String path; //同basePath变量一样
    private final String prelockName; //锁名称前缀，/locker下创建的顺序节点，例如以lock-开头，这样便于过滤无关节点
    private static final Integer MAX_RETRY_COUNT = 10; //最大重试次数

    public BaseDistributedLock(ZkClient client, String basePath, String prelockName) {
        this.client = client;
        this.basePath = basePath;
        this.path = basePath.concat("/").concat(prelockName);
        this.prelockName = prelockName;
    }

    /**
     * 删除节点
     */
    private void deletePath(String path) throws Exception {
        client.delete(path);
    }

    /**
     * 创建临时顺序节点
     */
    private String createEphemeralSequential(ZkClient client, String path) throws Exception{
        if (!client.exists(basePath)) {
            client.createPersistent(basePath, true); //创建锁持久节点
        }
        return client.createEphemeralSequential(path, null);
    }

    /**
     * 获取锁的核心方法
     */
    private boolean waitToLock (long startMills, Long millsToWait, String path) throws Exception {
        boolean haveTheLock = false; //获取锁标志
        boolean doDelete = false; //删除锁标志
        try {
            // ？？？这里的循环，通常情况下，是不是有的节点只执行一次，有的节点循环两次？？？
            // ？？？这里的while循环有没有必要，不是已经设置监听了吗？？？
            while (!haveTheLock) {
                //获取/locker节点下的所有顺序节点，并且从小到大排序
                List<String> children = getSortedChildren();
                //获取子节点，如/locker/node_0000000003返回node_0000000003
                String sequenceNodeName = path.substring(basePath.length() + 1);

                //计算刚才客户端创建的顺序节点在lokcer的所有子节点中排序位置，如果排序为0，则表示获取到了锁
                int ourIndex = children.indexOf(sequenceNodeName);

                /**
                 * 如果在getSortedChildren中没有找到之前创建的临时顺序节点，这表示可能由于网络闪断而导致Zookeeper认为连接断开而删除了我们创建的节点，此时需要抛出异常，让上一级去处理。上一级的做法是捕获该异常，并且执行重试指定的次数，见后面的 attempLock()方法
                 */
                if (ourIndex < 0) {
                    throw new ZkNoNodeException("节点没有找到：" + sequenceNodeName);
                }
                //如果当前客户端创建的节点在locker子节点列表中的位置大于0，表示其他客户端已经获取了锁，此时当前客户端需要等待其他客户端释放锁
                boolean isGetTheLock = ourIndex == 0; //是否得到锁

                //如何判断其他客户端是否已经释放了锁？从子节点列表获取比自己次小的那个节点，并对其建立监听
                String pathToWatch = isGetTheLock ? null : children.get(ourIndex - 1); //获取比自己次小的那个节点，如：node_000000002
                if (isGetTheLock) {
                    haveTheLock = true;
                } else {
                    // 等次小的节点被删除了，则表示当前客户端的节点应该是最小的了，所有使用CountDownLatch来实现等待
                    String previousSequencePath = basePath.concat("/").concat(pathToWatch);
                    final CountDownLatch latch = new CountDownLatch(1);
                    final IZkDataListener previousListener = new IZkDataListener() {
                        /**
                         * 监听指定节点数据发生变化时触发该方法
                         */
                        @Override
                        public void handleDataChange(String s, Object o) throws Exception {

                        }

                        /**
                         * 监听指定节点删除时触发该方法
                         * @param s
                         * @throws Exception
                         */
                        @Override
                        public void handleDataDeleted(String s) throws Exception {
                            //次小节点删除事件发生是，让countDownLatch结束等待
                            //此时还需要重新让程序回到while，重新判断一次
                            latch.countDown();
                        }
                    };


                    try {
                        //如果节点不存在会出现异常
                        client.subscribeDataChanges(previousSequencePath, previousListener);//注册监听

                        //发生超时需要删除节点
                        if(millsToWait != null){
                            millsToWait = (System.currentTimeMillis() - startMills);
                            startMills = System.currentTimeMillis();
                            if (millsToWait <= 0) {
                                doDelete = true;
                                break;
                            }
                            latch.await(millsToWait, TimeUnit.MICROSECONDS);
                        } else {
                            latch.await();
                        }
                    } catch (ZkNoNodeException e) {
                        e.printStackTrace();
                    } finally {
                        client.unsubscribeDataChanges(previousSequencePath, previousListener);
                    }
                }
            }
        } catch (Exception e) {
            //发生异常需要删除节点
            doDelete = true;
            throw e;
        } finally {
            //如果需要删除节点
            if (doDelete) {
                deletePath(path);
            }
        }
        return haveTheLock;
    }

    /**
     * 获取parentPath节点下的所有顺序节点，并且从小到大排序
     * @return
     */
    private List<String> getSortedChildren() {
        try {
            List<String> children = client.getChildren(basePath);
            Collections.sort(children, new Comparator<String>() {
                @Override
                public int compare(String lhs, String rhs) {
                    return getLockNodeNumer(lhs, prelockName).compareTo(getLockNodeNumer(rhs, prelockName));
                }
            });
            return children;
        } catch (ZkNoNodeException e) {
            client.createPersistent(basePath, true); //创建锁持久节点
            e.printStackTrace();
            return getSortedChildren();
        }
    }

    private String getLockNodeNumer(String str, String prelockName) {
        int index = str.lastIndexOf(prelockName);
        if (index >=0) {
            index += prelockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    /**
     * 释放锁
     */
    protected void releaseLock(String lockPath) throws Exception {
        deletePath(lockPath);
    }

    /**
     * 尝试获取锁
     */
    protected String attemptLock(long time, TimeUnit unit) throws Exception {
        final long startMills = System.currentTimeMillis();
        final Long millsToWait = (unit != null) ? unit.toMillis(time) : null;

        String ourPath = null;
        boolean hasTheLock = false; //获取锁标志
        boolean isDone = false; //是否完成得到锁
        int retryCount = 0; //重试次数

        //网络闪断需要重试一试
        while (!isDone) {
            isDone = true;
            try {
                ourPath = createEphemeralSequential(client, path);
                System.out.println(ourPath + "节点创建成功");
                /**
                 * 该方法用于判断自己是否获取到了锁，即自己创建的顺序节点在locker的所有子节点中是否最小
                 * 如果没有获取到锁，则等待其他客户端锁的释放，并且稍后重试直到获取到锁或者超时
                 */
                hasTheLock = waitToLock(startMills, millsToWait, ourPath);
            } catch (ZkNoNodeException e) {
                if (retryCount++ < MAX_RETRY_COUNT) {
                    isDone = false;
                } else {
                    throw e;
                }
            }
        }
        System.out.println(ourPath + "锁获取" + (hasTheLock ? "成功":"失败"));
        if (hasTheLock) {
            return ourPath;
        }
        return null;
    }
}
