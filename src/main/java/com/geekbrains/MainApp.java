package com.geekbrains;

import java.sql.*;

public class MainApp {
    private static Connection connection;
    private static Statement stmt;
    private static PreparedStatement psInsert;

    public static void connect() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection("jdbc:sqlite:main.db");
        stmt = connection.createStatement();
    }

    public static void disconnect() {
        try {
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void prepareAllStatement() throws SQLException {
        psInsert = connection.prepareStatement("INSERT INTO students (name, score) VALUES (?, ?);");
    }

    public static void main(String[] args) {
        try {
            connect();
            prepareAllStatement();
            clearTable();
            rollback();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            disconnect();
        }
    }

    private static void rollback() throws SQLException {
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob1', 75);");
        Savepoint sp1 = connection.setSavepoint();
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob2', 75);");
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob6', 75);");
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob4', 75);");
        connection.rollback(sp1);
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob3', 75);");
        connection.setAutoCommit(true);
    }

    public static void batchFillTable() throws SQLException {
        long start = System.currentTimeMillis();
        connection.setAutoCommit(false);
        for (int i = 1; i <= 2000; i++) {
            psInsert.setString(1, "Bob" + i);
            psInsert.setInt(2, (i * 5) % 100);
            psInsert.addBatch();
        }
        psInsert.executeBatch();
        connection.setAutoCommit(true);

        long end = System.currentTimeMillis();
        System.out.printf("time: %d ms", end - start);
    }

    public static void fillTable() throws SQLException {
        long start = System.currentTimeMillis();
        connection.setAutoCommit(false);
        for (int i = 1; i <= 2000; i++) {
            psInsert.setString(1, "Bob" + i);
            psInsert.setInt(2, (i * 5) % 100);
            psInsert.executeUpdate();
        }
        connection.setAutoCommit(true);
//        connection.commit();

        long end = System.currentTimeMillis();
        System.out.printf("time: %d ms", end - start);
    }

    //  CRUD create read update delete
    private static void selectEx() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT name,score FROM students \n" +
                "WHERE score >50;");

        while (rs.next()) {
            System.out.println(rs.getString("name") + " " + rs.getInt("score"));
        }
        rs.close();
    }

    private static void clearTable() throws SQLException {
        stmt.executeUpdate("DELETE FROM students;");
    }

    private static void deleteEx() throws SQLException {
        stmt.executeUpdate("DELETE FROM students WHERE id = 2;");
    }

    private static void updateEx() throws SQLException {
        stmt.executeUpdate("UPDATE students SET score = 60 WHERE score = 100;");
    }

    private static void insertEx() throws SQLException {
        stmt.executeUpdate("INSERT INTO students (name, score) VALUES ('Bob3', 75);");
    }


}
