package com.waitingforcode.model;

public class Game {

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static Game ofName(String name) {
        Game game = new Game();
        game.name = name;
        return game;
    }
}
