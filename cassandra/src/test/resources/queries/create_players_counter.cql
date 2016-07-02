CREATE TABLE players_default_counter (
    name text,
    teamName text,
    games counter,
    PRIMARY KEY (name, teamName)
)