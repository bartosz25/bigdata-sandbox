package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BulkTest extends CommonTestContext {

    private static final String NODE_1 = "/node_1";
    private static final String NODE_2 = "/node_2";

    @Test
    public void should_correctly_execute_bulk_znodes_creation() throws KeeperException, InterruptedException {
        zooKeeper.create(NODE_2, "Content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op createNode1 = Op.create(NODE_1, "content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op removeNode2 = Op.delete(NODE_2, ALL_VERSIONS);

        /**
         * {@code OpResult} is only an abstract class. It contains specific implementations for creation, delete,
         * data setting or version checking operations.
         */
        List<OpResult> results = zooKeeper.multi(Lists.newArrayList(createNode1, removeNode2));

        assertThat(results).hasSize(2);
        assertThat(results).extracting("type").containsOnly(ZooDefs.OpCode.create, ZooDefs.OpCode.delete);
        assertThat(results.stream().map(result -> result.getClass().getCanonicalName()).collect(Collectors.toList())).containsOnly(
                "org.apache.zookeeper.OpResult.CreateResult", "org.apache.zookeeper.OpResult.DeleteResult"
        );
        assertThat(zooKeeper.exists(NODE_1, false)).isNotNull();
        assertThat(zooKeeper.exists(NODE_2, false)).isNull();
    }

    @Test
    public void should_execute_only_some_valid_operations() throws KeeperException, InterruptedException {
        Op createNode1 = Op.create(NODE_1, "content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op createNode2 = Op.create(NODE_1, "content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op removeNode3 = Op.delete(NODE_2, ALL_VERSIONS);

        try {
            zooKeeper.multi(Lists.newArrayList(createNode1, createNode2, removeNode3));
            fail("Should fail on executing bulk operation with 1 failure");
        } catch (KeeperException.NodeExistsException exception) {
            List<OpResult> results = exception.getResults();

            assertThat(results).hasSize(3);
            assertThat(results).extracting("type").containsOnly(ZooDefs.OpCode.error);
            assertThat(results.stream().map(result -> result.getClass().getCanonicalName()).collect(Collectors.toList())).containsOnly(
                    "org.apache.zookeeper.OpResult.ErrorResult");
        }

        assertThat(zooKeeper.exists(NODE_1, false)).isNull();
    }

}
