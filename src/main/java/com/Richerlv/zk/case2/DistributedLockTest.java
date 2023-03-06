package com.Richerlv.zk.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author: Richerlv
 * @date: 2023/3/6 20:28
 * @description: 分布式锁测试类
 */

public class DistributedLockTest {

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        final DistributedLock lock1 = new DistributedLock();
        final DistributedLock lock2 = new DistributedLock();
        final DistributedLock lock3 = new DistributedLock();


        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("线程A获取到锁。。。");
                    Thread.sleep(5000);
                    lock1.unZkLock();
                    System.out.println("线程A释放锁。。。");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("线程B获取到锁。。。");
                    Thread.sleep(5000);
                    lock2.unZkLock();
                    System.out.println("线程B释放锁。。。");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

}
