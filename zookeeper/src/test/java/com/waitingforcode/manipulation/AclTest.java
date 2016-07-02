package com.waitingforcode.manipulation;

import com.waitingforcode.common.CommonTestContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.IPAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class AclTest extends CommonTestContext {

    private static final String ONLY_OWNER = "/only_owner";
    private static final String ONLY_OWNER_BIS = "/only_owner_bis";
    private static final String OWNER_WITHOUT_DIGEST = "/owner_without_digest";
    private static final String OWNER_IP = "/owner_ip";
    private static final String OWNER_IP_DIGEST = "/owner_ip_digest";
    private static final String OWNER_IP_READ_ONLY = "/owner_ip_read_only";
    private static final String OWNER_IP_READ_ONLY_FAILING = "/owner_ip_read_only_failing";
    private static final String OWNER_IP_FAILURE = "/owner_ip_auth_failure";
    private static final ArrayList<ACL> LOCALHOST_IP_AUTH = new ArrayList<>(
            Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1"))));

    @AfterClass
    public static void clean() throws InterruptedException {
        // remove created nodes
        try {
            safeDelete(OWNER_IP_READ_ONLY_FAILING, OWNER_IP_READ_ONLY, OWNER_IP_DIGEST, OWNER_IP_FAILURE, OWNER_IP, ONLY_OWNER,
                    ONLY_OWNER_BIS, OWNER_WITHOUT_DIGEST);
        } finally {
            zooKeeper.close();
        }
    }

    @Test(expected = KeeperException.NoAuthException.class)
    public void should_not_be_able_to_get_znode_content_for_other_user_than_its_creator() throws KeeperException, InterruptedException, IOException {
        zooKeeper.addAuthInfo("digest", "bartosz:my_password".getBytes());
        zooKeeper.create(ONLY_OWNER, "zNode owned by its creator".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT,
                (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("digest", "other_user:my_password".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }
        newSession.getData(ONLY_OWNER, false, new Stat());
    }

    @Test
    public void should_be_able_to_get_znode_content_when_new_session_is_created_for_the_same_user_as_znode_creator()
            throws KeeperException, InterruptedException, IOException {
        String zNodeContent = "zNode owned by its creator";
        zooKeeper.addAuthInfo("digest", "bartosz:my_password".getBytes());
        zooKeeper.create(ONLY_OWNER_BIS, zNodeContent.getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("digest", "bartosz:my_password".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        String content = new String(newSession.getData(ONLY_OWNER_BIS, false, new Stat()));

        assertThat(content).isEqualTo(zNodeContent);
    }

    @Test
    public void should_correctly_get_znode_with_authentication_ip() throws KeeperException, InterruptedException, IOException {
        String zNodeContent = "zNode owned by its creator";
        zooKeeper.addAuthInfo("ip", "127.0.0.1".getBytes());
        zooKeeper.create(OWNER_IP, zNodeContent.getBytes(), LOCALHOST_IP_AUTH, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("ip", "127.0.0.1".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        String content = new String(newSession.getData(OWNER_IP, false, new Stat()));

        assertThat(content).isEqualTo(zNodeContent);
    }

    /**
     * IP authentication is resolved by Apache ZooKeeper. So calling the same node with 2 different authentication
     * modes (one IP, other digest), should work.
     *
     * The authentication against ACL is made by {@link IPAuthenticationProvider#handleAuthentication(ServerCnxn, byte[])} where
     * IP is retrieved by:
     * <pre>
     * String id = cnxn.getRemoteSocketAddress().getAddress().getHostAddress();
     * cnxn.addAuthInfo(new Id(getScheme(), id));
     * </pre>
     */
    @Test
    public void should_get_node_even_if_uses_different_auth_information() throws KeeperException, InterruptedException, IOException {
        zooKeeper.addAuthInfo("ip", "127.0.0.1".getBytes());
        zooKeeper.create(OWNER_IP_DIGEST, "X".getBytes(), LOCALHOST_IP_AUTH, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("digest", "a:b".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        assertThat(newSession.getData(OWNER_IP_DIGEST, false, new Stat())).isEqualTo("X".getBytes());
    }

    @Test(expected = KeeperException.NoAuthException.class)
    public void should_fail_on_getting_znode_when_is_owned_by_other_ip_address() throws KeeperException, InterruptedException, IOException {
        ArrayList<ACL> otherIp = new ArrayList<>(
                Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("ip", "1.0.0.1"))));
        zooKeeper.addAuthInfo("ip", "127.0.0.1".getBytes());
        zooKeeper.create(OWNER_IP_FAILURE, "X".getBytes(), otherIp, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("ip", "127.0.0.1".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        newSession.getData(OWNER_IP_FAILURE, false, new Stat());
    }

    @Test
    public void should_allow_only_reading_znode_for_public() throws KeeperException, InterruptedException, IOException {
        ArrayList<ACL> worldReadOnlyACL = new ArrayList<>(
            Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
        zooKeeper.addAuthInfo("digest", "bartosz:pass".getBytes());
        zooKeeper.create(OWNER_IP_READ_ONLY, "X".getBytes(), worldReadOnlyACL, CreateMode.PERSISTENT);

        // open new session and try to get the file
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("digest", "bartosz2:pass2".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        assertThat(newSession.getData(OWNER_IP_READ_ONLY, false, new Stat())).isEqualTo("X".getBytes());
        newSession.setData(OWNER_IP_READ_ONLY, "Z".getBytes(), ALL_VERSIONS);
    }

    @Test(expected = KeeperException.NoAuthException.class)
    public void should_fail_on_modyfing_read_only_node() throws KeeperException, InterruptedException, IOException {
        ArrayList<ACL> readOnlyACL = new ArrayList<>(
                Collections.singletonList(new ACL(ZooDefs.Perms.READ, new Id("world", "anyone"))));
        zooKeeper.addAuthInfo("digest", "bartosz:pass".getBytes());
        zooKeeper.create(OWNER_IP_READ_ONLY_FAILING, "X".getBytes(), readOnlyACL, CreateMode.PERSISTENT);

        // open new session and try to set new file content
        ZooKeeper newSession = new ZooKeeper("127.0.0.1:2181", CONNECTION_TIMEOUT, (event) -> System.out.println("Processing event " + event));
        newSession.addAuthInfo("digest", "bartosz2:pass2".getBytes());
        while (newSession.getState() == ZooKeeper.States.CONNECTING) {
        }

        newSession.setData(OWNER_IP_READ_ONLY_FAILING, "Z".getBytes(), ALL_VERSIONS);
    }


    @Test(expected = KeeperException.InvalidACLException.class)
    public void should_fail_on_setting_creator_acl_without_auth_information() throws KeeperException, InterruptedException {
        zooKeeper.create("/failing_node", "Test".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
    }

}
