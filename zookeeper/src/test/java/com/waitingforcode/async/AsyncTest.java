package com.waitingforcode.async;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AsyncTest extends CommonTestContext {

    private static final String NODE_1 = "/node_1";
    private static final String NODE_2 = "/node_2";
    private static final String NODE_3 = "/node_3";

    @Test
    public void should_create_znode_asynchronously_and_call_callback() throws KeeperException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String contextObject = "Context -> string";
        int[] code = new int[1];
        String[] contextArray = new String[1];
        zooKeeper.create("/home_to_delete", "Home directory".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                (resultCode, path, context, name) -> {
                    code[0] = resultCode;
                    contextArray[0] = ""+context;
                    System.out.println("rc "+resultCode + " path "+path + " ctx "+context + " name "+name);
                    latch.countDown();
                }, contextObject);
        latch.await(5, TimeUnit.SECONDS);

        assertThat(code[0]).isEqualTo(KeeperException.Code.OK.intValue());
        assertThat(contextArray[0]).isEqualTo(contextObject);
    }

    @Test
    public void should_call_bulk_operation_with_callback() throws KeeperException, InterruptedException {
        zooKeeper.create(NODE_2, "Content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.create(NODE_3, "Data set".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op createNode1 = Op.create(NODE_1, "content".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Op removeNode2 = Op.delete(NODE_2, ALL_VERSIONS);
        Op setDataNode3 = Op.setData(NODE_3, "Other node data".getBytes(), ALL_VERSIONS);

        List<OpResult> resultsFromCallback = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        zooKeeper.multi(Lists.newArrayList(createNode1, removeNode2, setDataNode3),
                (returnCode, path, context, opResults) -> {
                    resultsFromCallback.addAll(opResults);
                    latch.countDown();
                }, null);

        latch.await(3, TimeUnit.SECONDS);

        assertThat(resultsFromCallback).hasSize(3);
        assertThat(resultsFromCallback).extracting("type")
                .containsOnly(ZooDefs.OpCode.create, ZooDefs.OpCode.delete, ZooDefs.OpCode.setData);
        assertThat(resultsFromCallback.stream().map(result -> result.getClass().getCanonicalName()).collect(Collectors.toList())).containsOnly(
                "org.apache.zookeeper.OpResult.CreateResult", "org.apache.zookeeper.OpResult.DeleteResult",
                "org.apache.zookeeper.OpResult.SetDataResult"
        );
        assertThat(zooKeeper.exists(NODE_1, false)).isNotNull();
        assertThat(zooKeeper.exists(NODE_2, false)).isNull();
        assertThat(new String(zooKeeper.getData(NODE_3, false, new Stat()))).isEqualTo("Other node data");
    }

}
