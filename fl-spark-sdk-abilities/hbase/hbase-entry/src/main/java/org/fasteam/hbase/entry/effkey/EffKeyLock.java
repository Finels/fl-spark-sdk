package org.fasteam.hbase.entry.effkey;

import org.apache.hadoop.hbase.shaded.org.apache.zookeeper.*;
import org.apache.hadoop.hbase.shaded.org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.hadoop.hbase.shaded.org.apache.zookeeper.data.Stat;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Description:  org.fasteam.hbase.entry
 *
 * @author FL
 * @version 1.0
 * @timestamp 2023/6/27
 */
public class EffKeyLock implements Lock, Watcher,AsyncCallback.StringCallback,AsyncCallback.StatCallback,AsyncCallback.ChildrenCallback {

    private final String ctx = "EffKey";
    private final String zkLockPath = "/effKey";
    private ZooKeeper zkCli;
    private String key;
    private String lockIndex;
    CountDownLatch cd = new CountDownLatch(1);
    public EffKeyLock(String key, int lockTimeout,ZkSupplier zkSupplier) throws IOException {
        this.zkCli = new ZooKeeperAdmin(zkSupplier.get(),lockTimeout,this);
        this.key = key;
    }
    public EffKeyLock(int lockTimeout,ZkSupplier zkSupplier) throws IOException {
        this.zkCli = new ZooKeeperAdmin(zkSupplier.get(),lockTimeout,this);
    }
    public void setKey(String key){
        this.key = key;
    }

    @Override
    public void lock() {
        if(this.key==null){
            throw new IllegalArgumentException("lock key is invalid.");
        }
        zkCli.create(zkLockPath,key.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,this,ctx);
        try {
            cd.await(6,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
        try {
            zkCli.delete(lockIndex,-1);
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){
            case None:
            case NodeCreated:
            case NodeDataChanged:
            case NodeChildrenChanged:
                break;
            case NodeDeleted:
                zkCli.getChildren("/",false,this,ctx);
                break;
        }
    }

    //exists call back
    @Override
    public void processResult(int resultCode, String path, Object ctx, Stat stat) {
        //TODO
    }

    //create lock callback
    @Override
    public void processResult(int resultCode, String path, Object ctx, String zkIndex) {
        this.lockIndex = zkIndex;
        zkCli.getChildren("/", false,this,ctx);
    }

    //get children callback
    @Override
    public void processResult(int resultCode, String path, Object ctx, List<String> children) {
        if (Objects.isNull(children)) {
            return;
        }
        Collections.sort(children);
        int i = children.indexOf(lockIndex.substring(1));
        if (Objects.equals(i, 0)) {
            cd.countDown();
        } else {
            zkCli.exists("/" + children.get(i - 1), this, this, ctx);
        }
    }
}
