CREATE TABLE players_teams_map (
    name text,
    teams map<text, text>,
    PRIMARY KEY (name)
)