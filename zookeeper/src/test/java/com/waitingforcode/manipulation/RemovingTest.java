package com.waitingforcode.manipulation;


import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.junit.AfterClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemovingTest extends CommonTestContext {

    private static final String HOME_TO_DELETE = "/home_to_delete";
    private static final String HOME_WITH_CHILDREN = "/home_with_children";
    private static final String HOME_WITH_CHILDREN_1 = "/home_with_children/1";
    private static final String HOME_WITH_CHILDREN_2 = "/home_with_children/2";

    @AfterClass
    public static void clean() throws InterruptedException {
        // remove created nodes
        try {
            safeDelete(HOME_TO_DELETE, HOME_WITH_CHILDREN_1, HOME_WITH_CHILDREN_2, HOME_WITH_CHILDREN);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_correctly_remove_znode_by_specific_version() throws KeeperException, InterruptedException {
        zooKeeper.create(HOME_TO_DELETE, "Home directory".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        int version = zooKeeper.exists(HOME_TO_DELETE, false).getVersion();

        zooKeeper.delete(HOME_TO_DELETE, version);

        assertThat(zooKeeper.exists(HOME_TO_DELETE, false)).isNull();
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void should_correctly_remove_znode_by_specific_version_which_is_not_current() throws KeeperException, InterruptedException {
        String path = "/home_multi_versions";
        zooKeeper.create(path, "Home directory".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        int version = zooKeeper.exists(path, false).getVersion();
        zooKeeper.setData(path, new byte[0], ALL_VERSIONS);

        zooKeeper.delete(path, version);
    }

    @Test(expected = KeeperException.NotEmptyException.class)
    public void should_fail_on_removing_znode_with_children_when_children_are_not_deleted_first() throws KeeperException, InterruptedException {
        createTree();

        zooKeeper.delete(HOME_WITH_CHILDREN, ALL_VERSIONS);
    }

    @Test
    public void should_correctly_remove_znode_when_children_are_removed_before() throws KeeperException, InterruptedException {
        createTree();
        zooKeeper.delete(HOME_WITH_CHILDREN_1, ALL_VERSIONS);
        zooKeeper.delete(HOME_WITH_CHILDREN_2, ALL_VERSIONS);
        zooKeeper.delete(HOME_WITH_CHILDREN, ALL_VERSIONS);

        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN, false)).isNull();
        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN_1, false)).isNull();
        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN_2, false)).isNull();
    }

    @Test
    public void should_delete_recursively_with_zkutils() throws KeeperException, InterruptedException {
        createTree();

        ZKUtil.deleteRecursive(zooKeeper, HOME_WITH_CHILDREN);

        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN, false)).isNull();
        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN_1, false)).isNull();
        assertThat(zooKeeper.exists(HOME_WITH_CHILDREN_2, false)).isNull();
    }

    private void createTree() throws KeeperException, InterruptedException {
        zooKeeper.create(HOME_WITH_CHILDREN, "Root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(HOME_WITH_CHILDREN_1, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(HOME_WITH_CHILDREN_2, "2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

}
