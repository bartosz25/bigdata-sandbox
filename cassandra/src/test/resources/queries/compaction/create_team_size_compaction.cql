CREATE TABLE simple_team (
    teamName text,
    city text,
    PRIMARY KEY (teamName)
) WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 2}
