package com.kam.zookeeperlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * zookeeper分布式锁实现，有序锁：根据服务获取顺序创建临时顺序节点，根据节点顺序来分配锁
 * 如果有一把锁，被多个人给竞争，此时多个人会排队，第一个拿到锁的人会执行，
 * 然后释放锁，后面的每个人都会去监听排在自己前面的那个人创建的node上，
 * 一旦某个人释放了锁，那么zookeeper会通知后边的一个人，一旦被通知那么就可以去获取锁了
 *
 * @author kamjin1996
 */
public class ZookeeperDistributeLock implements Watcher {

    private ZooKeeper zk;
    private String locksRoot = "/locks";
    private String productId;
    private String waitNode;
    private String lockNode;

    //这里的latch最好别放在这里，会出问题，最好是谁去监听，谁来创建
    private CountDownLatch latch;
    private CountDownLatch connectedLatch = new CountDownLatch(1);

    private int sessionTimeout = 30000;

    public ZookeeperDistributeLock(String productId) {
        this.productId = productId;
        try {
            String address = "192.168.31.187:2181,192.168.31.23:2181,192.168.31.142:2181";
            zk = new ZooKeeper(address, sessionTimeout, this);
            connectedLatch.await();
        } catch (IOException | InterruptedException e) {
            throw new LockException(e);
        }
    }

    //被监听回调的方法
    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedLatch.countDown();
            return;
        }
        //这里简单处理 实则比这个要复杂
        if (this.latch != null) {
            this.latch.countDown();
        }
    }

    public boolean tryLock() {
        try {
            //传进去的locksRoot+"/"+productId
            //假设productId代表了一个商品id，比如说1
            //locksRoot = locks;
            //此时如果每个服务都来获取锁，走这个方法，那么获取时zk会给他们分配顺序 如下
            // /locks/10000000000, /locks/10000000001,  /locks/10000000002,
            lockNode = zk.create(locksRoot + "/" + productId, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            //看看刚刚创建的节点是不是最小节点
            //locks: 10000000000 ， 10000000001 ，10000000002；
            List<String> locks = zk.getChildren(locksRoot, false);
            Collections.sort(locks);

            if (lockNode.equals(locksRoot + "/" + locks.get(0))) {
                //如果是最小节点，则表示获得锁
                return true;
            }

            //如果不是最小节点,找到比自己小1的节点
            int previousLockIndex = -1;
            for (int i = 0; i < locks.size(); i++) {
                if (lockNode.equals(locksRoot + "/" + locks.get(i))) {
                    previousLockIndex = i - 1;
                    break;
                }

            }

            this.waitNode = locks.get(previousLockIndex);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void acquireDistributedLock() {
        try {
            if (this.tryLock()) {
                return;
            } else {
                waitForLock(waitNode, sessionTimeout);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    private boolean waitForLock(String waitNode, long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(locksRoot + "/" + waitNode, true);
        if (stat != null) {
            this.latch = new CountDownLatch(1);
            this.latch.await(waitTime, TimeUnit.MILLISECONDS);
            //等待被countDown后继续执行
            this.latch = null;
        }
        return true;
    }

    public void unlock() {
        try {
            System.out.println("unlock" + lockNode);
            zk.delete(lockNode, -1);
            lockNode = null;
            zk.close();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }


}
