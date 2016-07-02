package com.waitingforcode.session;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionPathTest extends CommonTestContext {

    private static final String NODE_1 = "/node_1";
    private static final String NODE_2 = "/node_2";
    private static final String NODE_1_CHILD_1 = "/node_1/child_1";
    private static final String NODE_1_CHILD_2 = "/node_1/child_2";

    @BeforeClass
    public static void initContext() throws KeeperException, InterruptedException {
        zooKeeper.create(NODE_1, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(NODE_2, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(NODE_1_CHILD_1, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(NODE_1_CHILD_2, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @AfterClass
    public static void clean() throws InterruptedException {
        // remove created nodes
        try {
            safeDelete(NODE_1_CHILD_1, NODE_1_CHILD_2, NODE_1, NODE_2);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_connect_with_chroot_path() throws IOException, KeeperException, InterruptedException {
        /**
         * When a concrete path is specified at connection time, all further operations are relative to it.
         * It means that in our case we connect to /node_1 path. So accessing /child_1 looks for /node_1/child_1
         * zNode (existent one) but /node_1/child_1 tries to reach /node_1/node_1/child_1 (not existent)
         */
        ZooKeeper chrootKeeper = new ZooKeeper("127.0.0.1:2181"+NODE_1, CONNECTION_TIMEOUT,
                (event) -> System.out.println("Processing event " + event));
        while (chrootKeeper.getState() == ZooKeeper.States.CONNECTING) {
        }

        assertThat(chrootKeeper.exists(NODE_1_CHILD_1, false)).isNull();
        assertThat(chrootKeeper.exists("/child_1", false)).isNotNull();
    }

}
