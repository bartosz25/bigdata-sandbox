CREATE TABLE players_default_cached (
    name text,
    teamName text,
    PRIMARY KEY (name, teamName)
) WITH caching = {'keys':'ALL', 'rows_per_partition':'100'}