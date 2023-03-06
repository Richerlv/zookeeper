package com.Richerlv.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author: Richerlv
 * @date: 2023/3/6 14:08
 * @description: 创建zookeeper客户端
 */


public class zkClient {

    private static final Logger log = LoggerFactory.getLogger(com.Richerlv.zk.zkClient.class);

    //注意:逗号左右不能有空格
    private String connectString = "192.168.43.75:2181,192.168.43.116:2181,192.168.43.246:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
//        initLogRecord.initLog();

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    System.out.println("==========================================================");
                    children = zkClient.getChildren("/", true);
                    for(String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("===================================================");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        zkClient.create("/xl", "didi".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);
        for(String child : children) {
            System.out.println(child);
        }
        System.out.println("===================================================");

        //延时
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exists() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/dc", false);
        System.out.println(stat == null ? "not exists" : "exists");
    }
}
