package com.waitingforcode.model;

import java.util.Arrays;
import java.util.List;

public class Player {

    private List<Game> games;

    public List<Game> getGames() {
        return games;
    }

    public void setGames(List<Game> games) {
        this.games = games;
    }

    public static Player valueOf(Game...games) {
        Player player = new Player();
        List<Game> gamesList = Arrays.asList(games);
        player.setGames(gamesList);
        return player;
    }

}
