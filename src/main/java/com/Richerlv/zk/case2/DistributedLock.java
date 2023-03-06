package com.Richerlv.zk.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author: Richerlv
 * @date: 2023/3/6 19:54
 * @description: 基于zookeeper实现的分布式锁
 * 原理: 多个客户端向zookeeper中添加节点（临时、有序），最终序号最小的节点获取到锁，
 *      没有获取到锁的客户端监听前一个节点，直至前一个节点释放锁
 */

public class DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    private static ZooKeeper zkClient;

    private static String connectString = "192.168.43.75:2181,192.168.43.116:2181,192.168.43.246";
    private static int sessionTimeout = 2000;

    //创建连接同步器
    private CountDownLatch connectLatch = new CountDownLatch(1);
    //监听同步器
    private CountDownLatch waitLatch = new CountDownLatch(1);
    //前一节点
    String waitPath;
    //当前节点
    String currentNode;


    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        //建立连接
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //创建连接后，释放connectLatch
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }
                //前一节点下线后，释放waitLatch
                if(watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    System.out.println(currentNode + "release");
                    waitLatch.countDown();
                }
            }
        });
        //创建连接后才能继续执行
        connectLatch.await();

        //判断根节点是否存在，不存在则创建根节点 /locks
        Stat stat =  zkClient.exists("/locks", false);
        if(stat != null) {
            return;
        } else {
            //创建
            zkClient.create("/locks", "locks".getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    //获取锁
    public void zkLock() throws InterruptedException, KeeperException {
        //向zookeeper中注册节点
        currentNode = zkClient.create("/locks/" + "seq-", null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        //判断是不是只有一个节点
        List<String> children = zkClient.getChildren("/locks", false);
        if(children.size() == 1) {
            //获取到锁
            return;
        } else {
            //排序
            Collections.sort(children);
            //获取当前节点及其索引
            String thisNode = currentNode.substring("/locks/".length());
            int index = children.indexOf(thisNode);
            if(index == -1) {
                logger.error("数据异常");
            } else if(index == 0) {
                //索引为0，证明自己拿到了锁
                return;
            } else {
                //监听前一个节点
                waitPath = "/locks/"  + children.get(index - 1);
                zkClient.getData(waitPath, true, null);

                waitLatch.await();

                //等待监听结束
                return;
            }

        }

    }

    //释放锁
    public void unZkLock() throws InterruptedException, KeeperException {
        zkClient.delete(currentNode, -1);
    }

}
