package com.waitingforcode.session;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionRecoveryTest extends CommonTestContext {

    @Test
    public void should_see_ephemeral_nodes_after_reconnecting() throws KeeperException, InterruptedException, IOException {
        long sessionId = zooKeeper.getSessionId();
        byte[] sessionPassword = zooKeeper.getSessionPasswd();
        String path = zooKeeper.create("/tmp_ephemeral", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        assertThat(zooKeeper.exists(path, false)).isNotNull();

        System.out.println("Stop ZooKeeper now");
        Thread.sleep(5000);

        System.out.println("Waiting for new connection....Start ZooKeeper now");
        Thread.sleep(5000);
        ZooKeeper newZooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                (event) -> System.out.println("Processing event " + event), sessionId, sessionPassword);
        while (newZooKeeper.getState() != ZooKeeper.States.CONNECTED) {
        }

        assertThat(newZooKeeper.getSessionId()).isEqualTo(sessionId);
        assertThat(newZooKeeper.getSessionPasswd()).isEqualTo(sessionPassword);
        assertThat(newZooKeeper.getState().isConnected()).isTrue();
        assertThat(newZooKeeper.getState().isAlive()).isTrue();
        assertThat(newZooKeeper.exists("/sss", false)).isNull();
        assertThat(newZooKeeper.exists(path, false)).isNotNull();
    }

}
