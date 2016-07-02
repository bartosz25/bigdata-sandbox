package com.waitingforcode.util.model;

import com.datastax.driver.core.TupleValue;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.MoreObjects;

import java.util.List;

@Table(name = "players_multi_team")
public class MultiTeamPlayer {
    @PartitionKey
    @Column(name = "currentTeam")
    private String currentTeam;

    @ClusteringColumn
    @Column(name = "name")
    private String name;

    @Column(name = "fromYear")
    private int fromYear;

    @Frozen("frozen<list<team_history>>")
    private List<Team> teams;

    public void setCurrentTeam(String currentTeam) {
        this.currentTeam = currentTeam;
    }

    public String getCurrentTeam() {
        return currentTeam;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getFromYear() {
        return fromYear;
    }

    public void setFromYear(int fromYear) {
        this.fromYear = fromYear;
    }

    public List<Team> getTeams() {
        return teams;
    }

    public void setTeams(List<Team> teams) {
        this.teams = teams;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("currentTeam", currentTeam).add("name", name).toString();
    }

    @UDT(name = "team_history")
    public static class Team {
        @Field(name = "team")
        private String team;
        @Field(name = "years")
        private TupleValue years;
        private int fromYear;
        private int toYear;

        public void setTeam(String team) {
            this.team = team;
        }

        public void setFromYear(int fromYear) {
            this.fromYear = fromYear;
        }

        public void setToYear(int toYear) {
            this.toYear = toYear;
        }

        public String getTeam() {
            return team;
        }

        public int getFromYear() {
            return fromYear;
        }

        public int getToYear() {
            return toYear;
        }

        public TupleValue getYears() {
            return years;
        }

        public void setYears(TupleValue years) {
            this.years = years;
            this.fromYear = years.getInt(0);
            this.toYear = years.getInt(1);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("team", team).add("fromYear", fromYear)
                    .add("toYear", toYear).toString();
        }
    }

}
