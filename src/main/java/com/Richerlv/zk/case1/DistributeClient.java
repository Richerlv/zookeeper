package com.Richerlv.zk.case1;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author: Richerlv
 * @date: 2023/3/6 16:43
 * @description: 客户端
 */

public class DistributeClient {

    private static ZooKeeper zkClient;

    private static String connectString = "192.168.43.75:2181,192.168.43.116:2181,192.168.43.246";
    private static int sessionTimeout = 2000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeClient client = new DistributeClient();
        //创建zk连接
        client.getConnect();

        //监听/servers下的节点数量
        client.listen();

        //业务逻辑
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void listen() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/servers", true);

        ArrayList<String> childrenData = new ArrayList<>();
        for (String child : children) {
            byte[] childData = zkClient.getData("/servers/" + child, false, null);
            childrenData.add(new String(childData));
        }
        System.out.println(childrenData);
    }

    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    listen();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
