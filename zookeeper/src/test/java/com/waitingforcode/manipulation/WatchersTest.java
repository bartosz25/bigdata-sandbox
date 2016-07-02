package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class WatchersTest extends CommonTestContext {

    private static final String NODE_1 = "/node_1";
    private static final String PARENT_NODE = "/parent_1";
    private static final String CHILD_NODE = "/parent_1/child_1";
    private static final String DIFFERENT_THREAD_NODE = "/different_thread_node";

    @AfterClass
    public static void clean() throws InterruptedException {
        // remove created nodes
        try {
            // TODO : use ZkUtils !
            safeDelete(CHILD_NODE,  PARENT_NODE);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_prove_that_watches_are_one_time_events() throws KeeperException, InterruptedException {
        int[] calledEvents = new int[] {0};
        Watcher.Event.EventType[] types = new Watcher.Event.EventType[1];
        CountDownLatch latch = new CountDownLatch(2);
        zooKeeper.exists(NODE_1, new CountingWatcher(calledEvents, types, latch));

        zooKeeper.create(NODE_1, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.delete(NODE_1, ALL_VERSIONS);
        zooKeeper.create(NODE_1, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        latch.await(3, TimeUnit.SECONDS);

        assertThat(calledEvents[0]).isEqualTo(1);
        assertThat(types[0]).isEqualTo(Watcher.Event.EventType.NodeCreated);
    }

    @Test
    public void should_trigger_watch_on_node_delete() throws KeeperException, InterruptedException {
        int[] calledEvents = new int[] {0};
        Watcher.Event.EventType[] types = new Watcher.Event.EventType[1];
        zooKeeper.create(NODE_1, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        CountDownLatch latch = new CountDownLatch(1);
        /**
         * exists() method allows to trigger the most universal watcher. It applies to all
         * zNode operations: creation, removal or update. The only one not concerned operation
         * is about children - to register watcher for children nodes manipulation, we must use
         * getChildren().
         */
        zooKeeper.exists(NODE_1, new CountingWatcher(calledEvents, types, latch));

        zooKeeper.delete(NODE_1, ALL_VERSIONS);
        latch.await(3, TimeUnit.SECONDS);

        assertThat(calledEvents[0]).isEqualTo(1);
        assertThat(types[0]).isEqualTo(Watcher.Event.EventType.NodeDeleted);
    }

    @Test
    public void should_trigger_watch_on_children() throws KeeperException, InterruptedException {
        int[] calledEvents = new int[] {0};
        CountDownLatch latch = new CountDownLatch(1);
        Watcher.Event.EventType[] types = new Watcher.Event.EventType[1];
        zooKeeper.create(PARENT_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zooKeeper.getChildren(PARENT_NODE, new CountingWatcher(calledEvents, types, latch));
        zooKeeper.create(CHILD_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        latch.await(3, TimeUnit.SECONDS);

        assertThat(calledEvents[0]).isEqualTo(1);
        assertThat(types[0]).isEqualTo(Watcher.Event.EventType.NodeChildrenChanged);
    }

    @Test
    public void should_trigger_watch_on_different_session() throws KeeperException, InterruptedException {
        int[] calledEvents = new int[] {0, 0};
        Watcher.Event.EventType[] types = new Watcher.Event.EventType[2];
        zooKeeper.create(DIFFERENT_THREAD_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            ZooKeeper newZooKeeper = null;
            try {
                newZooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                        (event) -> System.out.println("Processing event " + event));
                while (newZooKeeper.getState() == ZooKeeper.States.CONNECTING) {
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                newZooKeeper.exists(DIFFERENT_THREAD_NODE, new CountingWatcher(calledEvents, types, latch));
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(2000);
        zooKeeper.delete(DIFFERENT_THREAD_NODE, ALL_VERSIONS);
        latch.await(5, TimeUnit.SECONDS);

        assertThat(calledEvents[0]).isEqualTo(1);
        assertThat(types[0]).isEqualTo(Watcher.Event.EventType.NodeDeleted);
    }

    @Test
    public void should_trigger_two_watches() throws KeeperException, InterruptedException {
        int[] calledEvents = new int[] {0};
        Watcher.Event.EventType[] types = new Watcher.Event.EventType[1];
        int[] calledEventsChildren = new int[] {0};
        Watcher.Event.EventType[] typesChildren = new Watcher.Event.EventType[1];
        zooKeeper.create(DIFFERENT_THREAD_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        CountDownLatch latch = new CountDownLatch(1);
        zooKeeper.exists(DIFFERENT_THREAD_NODE, new CountingWatcher(calledEvents, types, latch));
        zooKeeper.exists(DIFFERENT_THREAD_NODE, new CountingWatcher(calledEventsChildren, typesChildren, latch));

        Thread.sleep(2000);
        zooKeeper.delete(DIFFERENT_THREAD_NODE, ALL_VERSIONS);
        latch.await(5, TimeUnit.SECONDS);

        assertThat(calledEvents[0]).isEqualTo(1);
        assertThat(types[0]).isEqualTo(Watcher.Event.EventType.NodeDeleted);
        assertThat(calledEventsChildren[0]).isEqualTo(1);
        assertThat(typesChildren[0]).isEqualTo(Watcher.Event.EventType.NodeDeleted);

    }

    private static final class CountingWatcher implements Watcher {

        private int[] calledEvents;
        private Event.EventType[] types;
        private CountDownLatch latch;

        private CountingWatcher(int[] calledEvents, Event.EventType[] types, CountDownLatch latch) {
            this.calledEvents = calledEvents;
            this.types = types;
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {
            System.out.println("Processing event "+event.getPath()+" of type "+event.getType());
            calledEvents[0] = calledEvents[0]+1;
            types[0] = event.getType();
            latch.countDown();
        }
    }

}
