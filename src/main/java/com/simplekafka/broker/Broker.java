package com.simplekafka.broker;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class Broker {

    private final BrokerInfo info;
    private final ZooKeeperClient zkClient;
    private volatile boolean isController;

    public Broker(BrokerInfo info, String zkConnectString) {
        this.info = info;
        this.zkClient = new ZooKeeperClient(zkConnectString);
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        zkClient.connect();
        registerBroker();
        electController();
    }

    private void registerBroker() throws KeeperException, InterruptedException {
        String path = "/brokers/ids/" + info.getId();
        zkClient.createEphemeralNode(path, info.getHost() + ":" + info.getPort());
    }

    private void electController() {
        try {
            boolean won = zkClient.createEphemeralNode("/controller", String.valueOf(info.getId()));
            if (won) {
                isController = true;
                System.out.println("Broker " + info.getId() + " es el controller");
            } else {
                isController = false;
                zkClient.watchNode("/controller", this::electController);
            }
        } catch (KeeperException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isController() {
        return isController;
    }
}
