package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ZooKeeperClient implements Watcher {

    private final String connectString;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal;
    private static final int SESSION_TIMEOUT = 3000;

    public ZooKeeperClient(String connectString) {
        this.connectString = connectString;
    }

    public void connect() throws IOException, InterruptedException, KeeperException {
        connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
        connectedSignal.await();
        createPersistentNode("/brokers", "");
        createPersistentNode("/brokers/ids", "");
        createPersistentNode("/topics", "");
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.setData(path, data.getBytes(StandardCharsets.UTF_8), -1);
        }
    }

    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } else {
            return false;
        }
    }
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }
    public void watchChildren(String path, ChildrenCallback callback) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    watchChildren(path, callback);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        callback.onChildrenChanged(children);
    }
    public void watchNode(String path, NodeCallback callback) throws KeeperException, InterruptedException {
        zooKeeper.exists(path, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
                    event.getType() == Watcher.Event.EventType.NodeDataChanged ||
                    event.getType() == Watcher.Event.EventType.NodeCreated) {
                callback.onNodeChanged();
                try {
                    watchNode(path, callback);
                } catch (KeeperException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();

        } else if (event.getState() == Event.KeeperState.Disconnected) {
            // ZooKeeper already handles disconnections
        } else if (event.getState() == Event.KeeperState.Expired) {
            try {
                zooKeeper.close();
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
                connectedSignal.await();
            } catch (IOException | InterruptedException e) {
                throw  new RuntimeException(e);
            }
        }
    }
}
