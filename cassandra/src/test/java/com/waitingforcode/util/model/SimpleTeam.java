package com.waitingforcode.util.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.google.common.base.MoreObjects;

@Table(name = "simple_team")
public class SimpleTeam {

    @PartitionKey
    @Column(name = "teamName")
    private String teamName;

    @ClusteringColumn(0)
    @Column(name = "foundationYear")
    private int foundationYear;

    @ClusteringColumn(1)
    @Column(name = "country")
    private String country;

    private int division;

    @Transient
    private boolean euCountry;

    public SimpleTeam() {}

    public SimpleTeam(String teamName, int foundationYear, String country, int division) {
        this.teamName = teamName;
        this.foundationYear = foundationYear;
        this.country = country;
        this.division = division;
    }

    public String getTeamName() {
        return teamName;
    }

    public void setTeamName(String teamName) {
        this.teamName = teamName;
    }

    public int getFoundationYear() {
        return foundationYear;
    }

    public void setFoundationYear(int foundationYear) {
        this.foundationYear = foundationYear;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public boolean isEuCountry() {
        return "France".equals(country);
    }

    public int getDivision() {
        return division;
    }

    public void setDivision(int division) {
        this.division = division;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("teamName", teamName).add("division", division)
                .add("foundationYear", foundationYear).add("country", country).toString();
    }
}
