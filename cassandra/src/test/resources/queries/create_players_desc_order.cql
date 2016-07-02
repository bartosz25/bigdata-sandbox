CREATE TABLE players_descending (
    team text,
    teamName text,
    PRIMARY KEY (team, teamName)
) WITH CLUSTERING ORDER BY (teamName DESC)