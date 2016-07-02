CREATE TABLE players_multi_team (
    currentTeam text,
    fromYear int,
    teamName text,
    teams frozen<list<team_history>>,
    PRIMARY KEY (currentTeam, teamName)
)