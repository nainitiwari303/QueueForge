package com.queueforge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class DatabaseInspector {

    public static void main(String[] args) {
        String url = "jdbc:sqlite:queueforge.db";

        String sql = """
            SELECT id, name, priority, status, error_message
            FROM jobs
            ORDER BY id DESC
            LIMIT 20
            """;

        try (
            Connection connection = DriverManager.getConnection(url);
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(sql)
        ) {
            System.out.println("=== QueueForge Database ===");

            while (results.next()) {
                System.out.printf(
                    "ID: %d | Job: %s | Priority: %d | Status: %s | Error: %s%n",
                    results.getLong("id"),
                    results.getString("name"),
                    results.getInt("priority"),
                    results.getString("status"),
                    results.getString("error_message")
                );
            }

        } catch (Exception e) {
            System.err.println("Database inspection failed:");
            e.printStackTrace();
        }
    }
}