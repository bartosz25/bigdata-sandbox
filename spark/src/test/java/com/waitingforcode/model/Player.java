package com.waitingforcode.model;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

// Object used by Spark must implement Serializable
// Otherwise and exception like that is produced:
// Failed to serialize task 0, not attempting to retry it.
//        Exception during serialization:
//        java.io.NotSerializableException: com.waitingforcode.rdd.TransformationTest$Player
// Serialization stack:
// - object not serializable (class: com.waitingforcode.rdd.TransformationTest$Player
public class Player implements Serializable {
    private final String name;
    private final String team;
    private final int nationality;
    private final int goals;
    private final int assists;

    public Player(String name, String team, int nationality, int goals, int assists) {
        this.name = name;
        this.team = team;
        this.nationality = nationality;
        this.goals = goals;
        this.assists = assists;
    }

    public String getName() {
        return name;
    }

    public String getTeam() {
        return team;
    }

    public int getNationality() {
        return nationality;
    }

    public int getGoals() {
        return goals;
    }

    public int getAssists() {
        return assists;
    }

    public int getPoints() {
        return goals*2 + assists;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("name", name).add("team", team)
                .add("nationality", nationality).add("goals", goals).add("assists", assists).toString();
    }
}
