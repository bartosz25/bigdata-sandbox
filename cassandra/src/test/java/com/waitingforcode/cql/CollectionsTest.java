package com.waitingforcode.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.waitingforcode.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.waitingforcode.TestClient.SESSION;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class CollectionsTest {

    @BeforeClass
    public static void initDataset() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS collectionsTest");
        SESSION.execute("CREATE KEYSPACE collectionsTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE collectionsTest");
        String tableListQuery = TestHelper.readFromFile("/queries/collections/create_players_teams_list.cql");
        System.out.println("Executing query to create table: "+tableListQuery);
        SESSION.execute(tableListQuery);
        String tableSetQuery = TestHelper.readFromFile("/queries/collections/create_players_teams_set.cql");
        System.out.println("Executing query to create table: "+tableSetQuery);
        SESSION.execute(tableSetQuery);
        String tableMapQuery = TestHelper.readFromFile("/queries/collections/create_players_teams_map.cql");
        System.out.println("Executing query to create table: "+tableMapQuery);
        SESSION.execute(tableMapQuery);
        String tableFrozenQuery = TestHelper.readFromFile("/queries/collections/create_players_teams_frozen.cql");
        System.out.println("Executing query to create table: "+tableFrozenQuery);
        SESSION.execute(tableFrozenQuery);
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS collectionsTest");
    }

    @After
    public void cleanTables() {
        SESSION.execute("TRUNCATE TABLE collectionsTest.players_teams_list");
        SESSION.execute("TRUNCATE TABLE collectionsTest.players_teams_set");
        SESSION.execute("TRUNCATE TABLE collectionsTest.players_teams_map");
        SESSION.execute("TRUNCATE TABLE collectionsTest.players_teams_frozen_list");
    }

    @Test
    public void should_read_teams_in_the_order_of_data_adding() {
        insertList();

        ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_list");

        List<String> teams = resultSet.one().getList("teams", String.class);
        assertThat(teams).hasSize(6);
        assertThat(teams).containsExactly("SC Bastia", "Cagliari", "Modena", "AS Monaco", "Olympiakos", "SC Bastia");
    }

    @Test
    public void should_read_teams_in_sorted_order_from_set_column() {
        insertSet();

        ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_set");

        Set<String> teams = resultSet.one().getSet("teams", String.class);
        // Set doesn't accept duplicated data, so we have only 5 teams
        // In additionnaly, they are sorted in ascending order
        assertThat(teams).hasSize(5);
        assertThat(teams).containsExactly("AS Monaco", "Cagliari",  "Modena", "Olympiakos", "SC Bastia");
    }

    @Test
    public void should_read_map_in_order_of_data_definition() throws InterruptedException {
        insertMap();
        for (int i = 0; i < 10; i++) {
            ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_map");

            Map<String, String> teams = resultSet.one().getMap("teams", String.class, String.class);
            assertThat(teams).hasSize(7);
            assertThat(teams.values()).containsExactly("SC Bastia reserve", "SC Bastia", "Cagliari", "Modena", "AS Monaco",
                    "Olympiakos", "SC Bastia");
            assertThat(teams.keySet()).containsExactly("1997/1998", "1998/1999", "1999/2004", "2004/2004", "2004/2010",
                    "2010/2013", "2013/?");
            Thread.sleep(1000);
        }
    }

    @Test
    public void should_correctly_change_list_elements() {
        insertList();
        // Remove all Italian clubs and add 2 unknown clubs instead
        SESSION.execute("UPDATE collectionsTest.players_teams_list SET " +
                " teams  =  teams - ['Cagliari', 'Modena']  WHERE name = 'François Modesto'");
        SESSION.execute("UPDATE collectionsTest.players_teams_list SET " +
                " teams  = teams + ['Unknown_1', 'Unknown_2'] WHERE name = 'François Modesto'");

        ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_list");

        List<String> teams = resultSet.one().getList("teams", String.class);
        assertThat(teams).hasSize(6);
        assertThat(teams).containsExactly("SC Bastia", "AS Monaco", "Olympiakos", "SC Bastia", "Unknown_1", "Unknown_2");
    }

    @Test
    public void should_correctly_change_set_elements() {
        insertSet();
        // Remove all Italian clubs and add 2 unknown clubs instead
        SESSION.execute("UPDATE collectionsTest.players_teams_set SET " +
                " teams  =  teams - {'Cagliari', 'Modena'}  WHERE name = 'François Modesto'");
        SESSION.execute("UPDATE collectionsTest.players_teams_set SET " +
                " teams  = teams + {'Unknown_1', 'Unknown_2'} WHERE name = 'François Modesto'");

        ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_set");

        Set<String> teams = resultSet.one().getSet("teams", String.class);
        assertThat(teams).hasSize(5);
        assertThat(teams).containsExactly("AS Monaco", "Olympiakos", "SC Bastia", "Unknown_1", "Unknown_2");
    }

    @Test
    public void should_correctly_change_map_elements() throws InterruptedException {
        insertMap();
        // replace Italian teams by unknown ones
        SESSION.execute("UPDATE collectionsTest.players_teams_map SET " +
                " teams['1999/2004'] = 'Unknown_1'," +
                " teams['2004/2004'] = 'Unknown_2'  WHERE name = 'François Modesto'");


        for (int i = 0; i < 10; i++) {
            ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_map");

            Map<String, String> teams = resultSet.one().getMap("teams", String.class, String.class);
            assertThat(teams).hasSize(7);
            assertThat(teams.values()).containsExactly("SC Bastia reserve", "SC Bastia", "Unknown_1", "Unknown_2", "AS Monaco",
                    "Olympiakos", "SC Bastia");
            assertThat(teams.keySet()).containsExactly("1997/1998", "1998/1999", "1999/2004", "2004/2004", "2004/2010",
                    "2010/2013", "2013/?");
            Thread.sleep(1000);
        }
    }

    @Test
    public void should_correctly_delete_3_first_items_in_list() {
        insertList();

        SESSION.execute("DELETE teams[0], teams[1], teams[2]  FROM collectionsTest.players_teams_list WHERE name = 'François Modesto'");

        ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_list");

        List<String> teams = resultSet.one().getList("teams", String.class);
        assertThat(teams).hasSize(3);
        assertThat(teams).containsExactly("AS Monaco", "Olympiakos", "SC Bastia");
    }

    @Test(expected = SyntaxError.class)
    public void should_correctly_delete_3_first_items_in_set() {
        insertSet();

        // In sets, the remove can be done by indexes
        // Instead we should use '-' operator, already implemented
        // in previous tests
        // Other intuitive option, for example "DELETE teams{'Modena'} FROM..." won't work too.
        SESSION.execute("DELETE teams[0], teams[1], teams[2]  FROM collectionsTest.players_teams_set WHERE name = 'François Modesto'");
    }

    @Test
    public void should_correctly_delete_2_Italian_periods_of_player_from_map() throws InterruptedException {
        insertMap();

        // Map is not like a set and data can be removed from collection directly
        // by key name
        SESSION.execute("DELETE teams['1999/2004'], teams['2004/2004'] FROM collectionsTest.players_teams_map " +
                "WHERE name = 'François Modesto'");

        for (int i = 0; i < 10; i++) {
            ResultSet resultSet = SESSION.execute("SELECT * FROM collectionsTest.players_teams_map");

            Map<String, String> teams = resultSet.one().getMap("teams", String.class, String.class);
            assertThat(teams).hasSize(5);
            assertThat(teams.values()).containsExactly("SC Bastia reserve", "SC Bastia","AS Monaco",
                    "Olympiakos", "SC Bastia");
            assertThat(teams.keySet()).containsExactly("1997/1998", "1998/1999", "2004/2010",
                    "2010/2013", "2013/?");
            Thread.sleep(1000);
        }
    }

    @Test(expected = InvalidQueryException.class)
    public void should_not_allow_to_update_single_value_on_frozen_list() {
        SESSION.execute("INSERT INTO collectionsTest.players_teams_frozen_list (name, teams) " +
                " VALUES ('François Modesto', [(1998, 'SC Bastia'), (1999, 'Cagliari'), (2004, 'Modena'), " +
                "(2010, 'AS Monaco'), (2013, 'Olympiakos')]) ");

        SESSION.execute("UPDATE collectionsTest.players_teams_frozen_list SET " +
                " teams  = teams + [(1900, 'Not played yet')] WHERE name = 'François Modesto'");
    }

    private void insertList() {
        SESSION.execute("INSERT INTO collectionsTest.players_teams_list (name, teams) " +
                " VALUES ('François Modesto', ['SC Bastia', 'Cagliari', 'Modena', 'AS Monaco', 'Olympiakos', 'SC Bastia']) ");
    }

    private void insertSet() {
        SESSION.execute("INSERT INTO collectionsTest.players_teams_set (name, teams) " +
                " VALUES ('François Modesto', {'SC Bastia', 'Cagliari', 'Modena', 'AS Monaco', 'Olympiakos', 'SC Bastia'}) ");
    }

    private void insertMap() {
        SESSION.execute("INSERT INTO collectionsTest.players_teams_map (name, teams) " +
                " VALUES ('François Modesto', {'1998/1999': 'SC Bastia', '1999/2004': 'Cagliari', '2004/2004': 'Modena', " +
                "'2004/2010': 'AS Monaco', '2010/2013': 'Olympiakos', '2013/?': 'SC Bastia', '1997/1998': 'SC Bastia reserve'}) ");
    }

}
