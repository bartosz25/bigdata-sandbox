package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateTest extends CommonTestContext {

    private static final String ZNODE_CONTENT_UPDATE = "/home_content_update";
    private static final String ZNODE_CHILDREN_UPDATE = "/home_children_update";
    private static final String ZNODE_CHILDREN_UPDATE_CHILD = "/home_children_update/child_1";
    private static final String ZNODE_ACL_UPDATE = "/home_acl_update";
    private static final String ZNOODE_OLD_VERSION = "/home_old_version";

    @AfterClass
    public static void clean() throws InterruptedException {
        // remove created nodes
        try {
            safeDelete(ZNODE_CONTENT_UPDATE, ZNODE_CHILDREN_UPDATE_CHILD, ZNODE_CHILDREN_UPDATE, ZNODE_ACL_UPDATE);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_correctly_update_znode_content() throws KeeperException, InterruptedException {
        zooKeeper.create(ZNODE_CONTENT_UPDATE, "Home directory".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat initialStat = zooKeeper.exists(ZNODE_CONTENT_UPDATE, false);
        zooKeeper.setData(ZNODE_CONTENT_UPDATE, "New home directory".getBytes(), ALL_VERSIONS);

        Stat newStat = zooKeeper.exists(ZNODE_CONTENT_UPDATE, false);

        assertChanges(initialStat, newStat);
        // Children and ACL weren't changed in this test case, so they should be equal
        assertThat(initialStat.getCversion()).isEqualTo(newStat.getCversion());
        assertThat(initialStat.getAversion()).isEqualTo(newStat.getAversion());
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void should_fail_on_updating_only_old_version() throws KeeperException, InterruptedException {
        zooKeeper.create(ZNOODE_OLD_VERSION, "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Stat initialStat = zooKeeper.exists(ZNOODE_OLD_VERSION, false);
        // This update increases zNode version
        zooKeeper.setData(ZNOODE_OLD_VERSION, "3".getBytes(), ALL_VERSIONS);
        // And this one should fail since we try to update a version which is not current
        zooKeeper.setData(ZNOODE_OLD_VERSION, "2".getBytes(), initialStat.getVersion());
    }

    @Test
    public void should_correctly_update_children_node() throws KeeperException, InterruptedException {
        // create parent
        zooKeeper.create(ZNODE_CHILDREN_UPDATE, "Parent".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat initialParentStat = zooKeeper.exists(ZNODE_CHILDREN_UPDATE, false);
        // create and update child
        zooKeeper.create(ZNODE_CHILDREN_UPDATE_CHILD, "Child".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat initialChildStat = zooKeeper.exists(ZNODE_CHILDREN_UPDATE_CHILD, false);
        zooKeeper.setData(ZNODE_CHILDREN_UPDATE_CHILD, "New child".getBytes(), ALL_VERSIONS);

        // get new states after changes
        Stat newParentStat = zooKeeper.exists(ZNODE_CHILDREN_UPDATE, false);
        Stat newChildStat = zooKeeper.exists(ZNODE_CHILDREN_UPDATE_CHILD, false);

        assertChanges(initialChildStat, newChildStat);
        // Parent was not changed, so the version should be still the same
        assertThat(initialParentStat.getVersion()).isEqualTo(newParentStat.getVersion());
        assertThat(initialParentStat.getDataLength()).isEqualTo(newParentStat.getDataLength());
        assertThat(initialParentStat.getMtime()).isEqualTo(newParentStat.getMtime());
        assertThat(initialParentStat.getMzxid()).isEqualTo(newParentStat.getMzxid());
        assertThat(initialParentStat.getCzxid()).isEqualTo(newParentStat.getCzxid());
        assertThat(initialParentStat.getCtime()).isEqualTo(newParentStat.getCtime());
        // Unlike in previous test, children were changed, so the children version should be incremented
        assertThat(initialParentStat.getCversion()).isEqualTo(newParentStat.getCversion());
        // Children and ACL weren't changed in this test case, so they should be equal
        assertThat(initialParentStat.getAversion()).isEqualTo(newParentStat.getAversion());
    }

    @Test
    public void should_correctly_detect_acl_changes() throws KeeperException, InterruptedException {
        zooKeeper.create(ZNODE_ACL_UPDATE, "ACL".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Stat initialStat = zooKeeper.exists(ZNODE_ACL_UPDATE, false);
        zooKeeper.setACL(ZNODE_ACL_UPDATE, ZooDefs.Ids.READ_ACL_UNSAFE, ALL_VERSIONS);

        Stat newStat = zooKeeper.exists(ZNODE_ACL_UPDATE, false);

        assertThat(initialStat.getVersion()).isEqualTo(newStat.getVersion());
        assertThat(initialStat.getDataLength()).isEqualTo(newStat.getDataLength());
        assertThat(initialStat.getMtime()).isEqualTo(newStat.getMtime());
        assertThat(initialStat.getMzxid()).isEqualTo(newStat.getMzxid());
        assertThat(initialStat.getCzxid()).isEqualTo(newStat.getCzxid());
        assertThat(initialStat.getCtime()).isEqualTo(newStat.getCtime());
        assertThat(initialStat.getCversion()).isEqualTo(newStat.getCversion());
        // This time ACL was modified, so it should be reflected on this test
        assertThat(initialStat.getAversion()).isNotEqualTo(newStat.getAversion());

    }

    private static void assertChanges(Stat oldStat, Stat newStat) {
        // According to doc, version is bumped
        assertThat(oldStat.getVersion()).isNotEqualTo(newStat.getVersion());
        // New data should be defined too
        assertThat(oldStat.getDataLength()).isNotEqualTo(newStat.getDataLength());
        // Modification stats (last time and last id) should be different
        assertThat(oldStat.getMtime()).isNotEqualTo(newStat.getMtime());
        assertThat(oldStat.getMzxid()).isNotEqualTo(newStat.getMzxid());
        // But creation date should remain the same
        assertThat(oldStat.getCzxid()).isEqualTo(newStat.getCzxid());
        assertThat(oldStat.getCtime()).isEqualTo(newStat.getCtime());
    }

}
