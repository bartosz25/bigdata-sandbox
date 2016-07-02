package com.waitingforcode.util.model;

import com.datastax.driver.core.Row;
import com.google.common.base.MoreObjects;

public class Player {

    private String team;

    private String name;

    public String getTeam() {
        return team;
    }

    public String getName() {
        return name;
    }

    public static Player fromRow(Row row) {
        Player player = new Player();
        player.team = row.getString("team");
        player.name = row.getString("name");
        return player;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("team", team).add("name", name).toString();
    }
}
