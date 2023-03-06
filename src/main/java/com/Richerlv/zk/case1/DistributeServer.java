package com.Richerlv.zk.case1;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author: Richerlv
 * @date: 2023/3/6 16:35
 * @description: 服务器
 */

public class DistributeServer {

    private static ZooKeeper zkClient;

    private static String connectString = "192.168.43.75:2181,192.168.43.116:2181,192.168.43.246";
    private static int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer server = new DistributeServer();
        //创建zk连接
        server.getConnect();

        //注册节点到/servers
        server.register(args[0]);

        //业务逻辑
        server.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void register(String hostname) throws InterruptedException, KeeperException {
        zkClient.create("/servers/" + hostname, hostname.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online");
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
