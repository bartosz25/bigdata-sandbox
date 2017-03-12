package com.waitingforcode.sql;

import org.h2.tools.DeleteDbFiles;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.waitingforcode.sql.DatasetTest.City.valueOf;

class InMemoryDatabase {

    public static final String DB_NAME = "testdb";
    public static final String DB_DRIVER = "org.h2.Driver";
    public static final String DB_CONNECTION = "jdbc:h2:~/" + DB_NAME;
    public static final String DB_USER = "root";
    public static final String DB_PASSWORD = "";

    public static void init() {
        DeleteDbFiles.execute("~", DB_NAME, true);
        String createTableQuery = "CREATE TABLE city(name varchar(30) primary key, country varchar(50), " +
                "continent varchar(50))";
        try (Connection connection = getDBConnection()) {
            connection.setAutoCommit(false);

            PreparedStatement createPreparedStatement = connection.prepareStatement(createTableQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();

            List<DatasetTest.City> cities = Arrays.asList(valueOf("London", "England", "Europe"),
                    valueOf("Metz", "France", "Europe"), valueOf("Poznan", "Poland", "Europe"),
                    valueOf("Bogota", "Colombia", "South America"), valueOf("Chicago", "USA", "North America"),
                    valueOf("Madrid", "Spain", "Europe"), valueOf("Budapest", "Hungary", "Europe")
            );
            cities.forEach(city -> insertCity(connection, city));
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertCity(Connection connection, DatasetTest.City city) {
        String insertDataQuery = "INSERT INTO city (name, country, continent) values (?,?, ?)";
        try {
            PreparedStatement insertPreparedStatement = connection.prepareStatement(insertDataQuery);
            insertPreparedStatement.setString(1, city.getName());
            insertPreparedStatement.setString(2, city.getCountry());
            insertPreparedStatement.setString(3, city.getContinent());
            insertPreparedStatement.executeUpdate();
            insertPreparedStatement.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection getDBConnection() {
        try {
            Class.forName(DB_DRIVER);
            Connection dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            return dbConnection;
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
