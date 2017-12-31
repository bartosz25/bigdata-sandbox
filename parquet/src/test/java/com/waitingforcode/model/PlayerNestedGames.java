package com.waitingforcode.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PlayerNestedGames {
    private List<List<Game>> games;

    public List<List<Game>> getGames() {
        return games;
    }

    public void setGames(List<List<Game>> games) {
        this.games = games;
    }

    public static PlayerNestedGames valueOf(Game...games) {
        PlayerNestedGames player = new PlayerNestedGames();
        List<List<Game>> nestedGames = new ArrayList<>();
        List<Game> gamesList = Arrays.asList(games);
        nestedGames.add(gamesList);
        player.setGames(nestedGames);
        return player;
    }

}
