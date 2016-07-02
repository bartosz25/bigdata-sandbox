CREATE TABLE players_teams_set (
    name text,
    teams set<text>,
    PRIMARY KEY (name)
)