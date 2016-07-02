package com.waitingforcode.session;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionTest {

    private static final int CONNECTION_TIMEOUT = 5000;

    @Test
    public void should_correctly_connect_to_zookeeper() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                    (event) -> System.out.println("Processing event " + event));
            /**
             * We must wait some time because connection process is asynchronous, ie. ZooKeeper
             * instance is returned before connection is really done.
             */
            while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
            }

            assertThat(zooKeeper.getState().isConnected()).isTrue();
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_fail_on_connecting_to_not_existent_server() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:3210", CONNECTION_TIMEOUT,
                    (event) -> System.out.println("Processing event " + event));
            ZooKeeper.States state = zooKeeper.getState();


            assertThat(state.isConnected()).isFalse();
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_override_session_timeout_when_it_is_lower_than_tick_time() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper("127.0.0.1:2181", 100,
                    (event) -> System.out.println("Processing event " + event));
            while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
            }
            System.out.println("Session timeout is "+zooKeeper.getSessionTimeout());

            assertThat(zooKeeper.getSessionTimeout()).isNotEqualTo(100);
        } finally {
            zooKeeper.close();
        }
    }

    @Test
    public void should_not_override_session_timeout_when_it_is_lower_than_tick_time() throws IOException, InterruptedException {
        ZooKeeper zooKeeper = null;
        try {
            // tick time is configured to 2000 ms, so 30000 ms of session timeout should be enough to make
            // ZooKeeper not overriding this value
            zooKeeper = new ZooKeeper("127.0.0.1:2181", 30000,
                    (event) -> System.out.println("Processing event " + event));
            while (zooKeeper.getState() == ZooKeeper.States.CONNECTING) {
            }
            System.out.println("Session timeout is "+zooKeeper.getSessionTimeout());

            assertThat(zooKeeper.getSessionTimeout()).isEqualTo(30000);
        } finally {
            zooKeeper.close();
        }
    }
}
