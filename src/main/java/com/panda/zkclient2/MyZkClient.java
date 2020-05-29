package com.panda.zkclient2;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class MyZkClient extends ZkClient {

    public MyZkClient(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer) {
        super(zkServers, sessionTimeout, connectionTimeout, zkSerializer);
    }

    @Override
    public void watchForData(final String path) {
        retryUntilConnected(() -> {
            System.out.println("进入重写的watchForData方法了！要被监听的节点path是:" + path);
            Stat stat = new Stat();
            _connection.readData(path, stat, true);
            // 监听节点时，若节点不存在 则抛出异常
            if(!exists(path)){
                throw new KeeperException.NoNodeException();
            }
            return null;
        });
    }

}
