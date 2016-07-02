package com.waitingforcode.util;


import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public final class ZooKeeperHelper {

    private static final int CONNECTION_TIMEOUT = 25000;

    private ZooKeeperHelper() {
        // prevents init
    }

    public static ZooKeeper openSession() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = null;
        zooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                (event) -> System.out.println("Processing event " + event));
        while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
        }
        return zooKeeper;
    }

}
