package com.waitingforcode.client;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectionTest {

    @Test
    public void should_correctly_connect_to_cassandra_node() {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect();

        assertThat(session.getState().getConnectedHosts()).hasSize(1);
        assertThat(session.isClosed()).isFalse();
    }

    @Test
    public void should_not_fail_to_connect_to_cluster_with_not_existent_cluster_name_and_good_contact_point() {
        // It doesn't fail because specified cluster name is not the same as the real Cassandra's cluster name
        // coming from cassandra.yaml configuration file
        // Instead, this name relates to created Cluster instance
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withClusterName("X").build();
        Session session = cluster.connect();

        assertThat(session.getState().getConnectedHosts()).hasSize(1);
        assertThat(session.isClosed()).isFalse();
    }

    @Test(expected = InvalidQueryException.class)
    public void should_fail_to_connecting_to_not_existing_keyspace() {
        // It fails because 'Z' keyspace doesn't exist
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        cluster.connect("Z");
    }

    @Test(expected = NoHostAvailableException.class)
    public void should_fail_to_connect_to_not_started_node() {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.2").build();
        cluster.connect();
    }

}
