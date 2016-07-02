package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class WritingTest extends CommonTestContext {

    private static final String MIXED_PARENT_FILE_NODE = "/home_mixed_parent_file";

    @AfterClass
    public static void clean() throws InterruptedException, KeeperException {
        // remove created nodes
        try {
            ZKUtil.deleteRecursive(zooKeeper, MIXED_PARENT_FILE_NODE);
            safeDelete("/home", "/home_to_delete", "/home_with_children/1", "/home_with_children/2", "/home_with_children",
                    "/duplicated_node");
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_correctly_create_persistent_new_znode() throws KeeperException, InterruptedException {
        /**
         * Created node is not ephemeral. It means that without clean() method call, the next run of this test
         * would fail. Persistent zNodes, unlike ephemeral, are stored in persistent way, event after ZooKeeper
         * server restart.
         */
        String path = zooKeeper.create("/home", "Home directory".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        assertThat(path).isEqualTo("/home");
        Stat nodeStat = zooKeeper.exists("/home", false);
        assertThat(nodeStat).isNotNull();
        assertThat(nodeStat.getEphemeralOwner()).isEqualTo(0L);
    }

    @Test
    public void should_detect_node_as_not_existent() throws KeeperException, InterruptedException {
        assertThat(zooKeeper.exists("/fake_node", false)).isNull();
    }

    @Test
    public void should_correctly_create_ephemeral_node() throws KeeperException, InterruptedException, IOException {
        /**
         * Ephemeral zNode doesn't need manual clean up in clean() method. It's because it's automatically
         * deleted by ZooKeeper at the end of session.
         */
        String path = zooKeeper.create("/home_ephemeral", "Home ephemeral directory".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        assertThat(path).isEqualTo("/home_ephemeral");
        Stat nodeStat = zooKeeper.exists("/home_ephemeral", false);
        assertThat(nodeStat).isNotNull();
        assertThat(nodeStat.getEphemeralOwner()).isNotEqualTo(0L);
        zooKeeper.close();

        /**
         * Ephemeral zNodes live only within session which created them. After closing this session, thing what
         * it's done one line before, these zNodes aren't anymore reachable.
         */
        zooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, null);
        while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
        }
        assertThat(zooKeeper.exists("/home_ephemeral", false)).isNull();
    }

    @Test
    public void should_correctly_create_children() throws KeeperException, InterruptedException {
        zooKeeper.create("/home_with_children", "Root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/home_with_children/1", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/home_with_children/2", "2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        List<String> children = zooKeeper.getChildren("/home_with_children", false);

        assertThat(children).hasSize(2);
        assertThat(children).containsOnly("1", "2");
    }

    @Test(expected = KeeperException.NoChildrenForEphemeralsException.class)
    public void should_fail_on_creating_children_on_ephemeral_node() throws KeeperException, InterruptedException {
        zooKeeper.create("/home_ephemeral_with_children", "Root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.create("/home_ephemeral_with_children/1", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @Test(expected = KeeperException.NodeExistsException.class)
    public void should_fail_on_creating_the_same_znode_twice() throws KeeperException, InterruptedException {
        zooKeeper.create("/duplicated_node", "duplicated content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/duplicated_node", "duplicated content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void should_get_correct_data_of_created_znode() throws KeeperException, InterruptedException, IOException {
        String path = "/home_data_ephemeral";
        zooKeeper.create(path, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(path, false, stat);

        assertThat(new String(data)).isEqualTo("1");
        assertThat(stat.getDataLength()).isEqualTo(1);
    }

    @Test
    public void should_get_correct_data_of_created_directory_znode() throws KeeperException, InterruptedException, IOException {
        zooKeeper.create(MIXED_PARENT_FILE_NODE, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String childrenPath =
                zooKeeper.create(MIXED_PARENT_FILE_NODE+"/1", "2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(MIXED_PARENT_FILE_NODE, false, stat);
        List<String> children = zooKeeper.getChildren(MIXED_PARENT_FILE_NODE, false);
        Stat childStat = zooKeeper.exists(childrenPath, false);


        assertThat(new String(data)).isEqualTo("1");
        assertThat(stat).isNotNull();
        assertThat(stat.getDataLength()).isEqualTo(1);
        assertThat(children).hasSize(1);
        assertThat(children.get(0)).isEqualTo("1");
        assertThat(childStat).isNotNull();
        assertThat(childStat.getDataLength()).isEqualTo(1);
    }

    @Test(expected = KeeperException.ConnectionLossException.class)
    public void should_fail_on_saving_znode_bigger_than_1_mb() throws KeeperException, InterruptedException {
        zooKeeper.create("/home_big_file", makeBigFile(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private byte[] makeBigFile() {
        int mb = 1024 * 1024;
        byte[] content = new byte[mb];
        for (int i = 0; i < mb; i++) {
            content[i] = Byte.valueOf("1");
        }
        return content;
    }

}
