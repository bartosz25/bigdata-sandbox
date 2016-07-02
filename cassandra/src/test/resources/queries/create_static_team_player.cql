CREATE TABLE static_team_player (
    teamName text,
    player text,
    foundationYear int static,
    country text static,
    division int static,
    bornYear int,
    PRIMARY KEY (teamName, player)
)