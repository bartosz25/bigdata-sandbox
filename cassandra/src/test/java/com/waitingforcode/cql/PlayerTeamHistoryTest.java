package com.waitingforcode.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.waitingforcode.TestHelper;
import com.waitingforcode.util.model.MultiTeamPlayer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static com.waitingforcode.TestClient.*;
import static org.assertj.core.api.Assertions.assertThat;

public class PlayerTeamHistoryTest {

    @BeforeClass
    public static void initDataset() throws IOException, URISyntaxException {
        SESSION.execute("DROP KEYSPACE IF EXISTS playerTeamHistoryTest");
        SESSION.execute("CREATE KEYSPACE playerTeamHistoryTest WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        SESSION.execute("USE playerTeamHistoryTest");
        String teamHistoryTypeQuery = TestHelper.readFromFile("/queries/create_type_team_history.cql");
        System.out.println("Executing query to create user type: "+teamHistoryTypeQuery);
        SESSION.execute(teamHistoryTypeQuery);
        String tableQuery = TestHelper.readFromFile("/queries/create_players_team.cql");
        System.out.println("Executing query to create table: "+tableQuery);
        SESSION.execute(tableQuery);
    }

    @AfterClass
    public static void destroy() {
        SESSION.execute("DROP KEYSPACE IF EXISTS playerTeamHistoryTest");
    }

    @Test
    public void should_correctly_add_player_to_table_with_tuple_set_and_user_defined_type() throws InterruptedException {
        SESSION.execute("INSERT INTO playerTeamHistoryTest.players_multi_team (currentTeam, fromYear, name, teams) " +
                " VALUES ('SC Bastia', 2013, 'François Modesto', " +
                "[{team: 'SC Bastia', years: (1998, 1999)}, {team: 'Cagliari', years: (1999, 2004)}, " +
                "{team: 'Modena', years: (2004, 2004)}, {team: 'AS Monaco', years: (2004, 2010)}, " +
                "{team: 'Olympiakos', years: (2010, 2013)}] )");

        ResultSet resultSet = SESSION.execute("SELECT * FROM playerTeamHistoryTest.players_multi_team");

        Mapper<MultiTeamPlayer> mapper = MAPPING_MANAGER.mapper(MultiTeamPlayer.class);
        Result<MultiTeamPlayer> players = mapper.map(resultSet);
        List<MultiTeamPlayer> playersList = players.all();
        assertThat(playersList).hasSize(1);
        MultiTeamPlayer francoisModesto = playersList.get(0);
        assertThat(francoisModesto.getCurrentTeam()).isEqualTo("SC Bastia");
        assertThat(francoisModesto.getName()).isEqualTo("François Modesto");
        assertThat(francoisModesto.getFromYear()).isEqualTo(2013);
        assertThat(francoisModesto.getTeams()).hasSize(5);
        assertThat(francoisModesto.getTeams()).extracting("team").containsExactly("SC Bastia", "Cagliari", "Modena", "AS Monaco",
                "Olympiakos");
        assertThat(francoisModesto.getTeams()).extracting("fromYear").containsExactly(1998, 1999, 2004, 2004, 2010);
        assertThat(francoisModesto.getTeams()).extracting("toYear").containsExactly(1999, 2004, 2004, 2010, 2013);
    }

}
