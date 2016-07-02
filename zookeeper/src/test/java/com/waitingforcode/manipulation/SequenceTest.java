package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.junit.AfterClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SequenceTest extends CommonTestContext {

    private static final String PARENT_NODE = "/home_parent"+System.currentTimeMillis();
    private static final String PARENT_NODE_2 = "/home_parent2"+System.currentTimeMillis();

    @AfterClass
    public static void clean() throws InterruptedException, KeeperException {
        // remove created nodes
        try {
            ZKUtil.deleteRecursive(zooKeeper, PARENT_NODE);
            ZKUtil.deleteRecursive(zooKeeper, PARENT_NODE_2);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_correctly_create_sequence_node() throws KeeperException, InterruptedException {
        /**
         * Sequence applies always from parent zNode. In additionally, it increments on every created zNode.
         * It's the reason why the 4th created sequential children hasn't counter at 2 but at 3 (counting starts by 0).
         * On creating the single one not-sequential children, the counter increments too. So, the last sequential node can't
         * be associated to 2.
         * Simply speaking, we could though that sequence counter corresponds to the formula (number of zNodes - 1).
         */
        zooKeeper.create(PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String path1 = zooKeeper.create(PARENT_NODE+PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String path2 = zooKeeper.create(PARENT_NODE+PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String path3 = zooKeeper.create(PARENT_NODE+PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        String path4 = zooKeeper.create(PARENT_NODE+PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created paths: "+path1+", "+path2+", "+path3+", "+path4);

        assertThat(zooKeeper.exists(PARENT_NODE+PARENT_NODE+"0000000000", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE+PARENT_NODE+"0000000001", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE+PARENT_NODE+"0000000002", false)).isNull();
        assertThat(zooKeeper.exists(PARENT_NODE+PARENT_NODE, false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE+PARENT_NODE+"0000000003", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE, false)).isNotNull();
    }

    @Test
    public void should_correctly_create_sequential_node_if_not_sequential_node_was_created_before()
            throws KeeperException, InterruptedException {
        /**
         * This time we try to create a normal ephemeral node which name is the same as the name of
         * the expecting first ephemeral sequential node. As you can see below, ZooKeeper doesn't fail
         * and creates new sequential zNode with suffix ending by 1.
         */
        zooKeeper.create(PARENT_NODE_2, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String path1 = zooKeeper.create(PARENT_NODE_2+PARENT_NODE_2+"0000000000",
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        String path2 = zooKeeper.create(PARENT_NODE_2+PARENT_NODE_2, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        String path3 = zooKeeper.create(PARENT_NODE_2+PARENT_NODE_2+"xyz", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        String path4 = zooKeeper.create(PARENT_NODE_2+PARENT_NODE_2, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created paths: "+path1+", "+path2+", "+path3+", "+path4);

        assertThat(zooKeeper.exists(PARENT_NODE_2+PARENT_NODE_2+"0000000000", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE_2+PARENT_NODE_2+"0000000001", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE_2+PARENT_NODE_2+"xyz", false)).isNotNull();
        assertThat(zooKeeper.exists(PARENT_NODE_2+PARENT_NODE_2+"0000000003", false)).isNotNull();

    }

}
