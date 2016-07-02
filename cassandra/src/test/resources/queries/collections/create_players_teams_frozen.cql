CREATE TABLE players_teams_frozen_list (
    name text,
    teams frozen<list<tuple<int, text>>>,
    PRIMARY KEY (name)
)