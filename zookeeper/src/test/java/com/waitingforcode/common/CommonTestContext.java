package com.waitingforcode.common;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.BeforeClass;

import java.io.IOException;

public class CommonTestContext {

    public static final int CONNECTION_TIMEOUT = 5000;
    public static final int ALL_VERSIONS = -1;
    protected static ZooKeeper zooKeeper = null;

    @BeforeClass
    public static void connect() throws IOException, KeeperException, InterruptedException {
        zooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                (event) -> System.out.println("Processing event " + event));
        /**
         * We must wait some time because connection process is asynchronous, ie. ZooKeeper
         * instance is returned before connection is really done.
         */
        while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
        }
    }

    protected static void safeDelete(String...paths) {
        for (String path : paths) {
            try {
                zooKeeper.delete(path, ALL_VERSIONS);
            } catch (InterruptedException|KeeperException e) {
                e.printStackTrace();
            }
        }
    }

}
