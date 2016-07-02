CREATE TABLE simple_team (
    teamName text,
    foundationYear int,
    country text,
    division int,
    PRIMARY KEY (teamName, foundationYear, country)
)