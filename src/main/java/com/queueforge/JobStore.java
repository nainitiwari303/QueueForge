
package com.queueforge;

import java.sql.*;
import java.util.Optional;

public class JobStore implements AutoCloseable {

    private final Connection connection;

    public JobStore(String databasePath) throws SQLException {
        connection = DriverManager.getConnection(
                "jdbc:sqlite:" + databasePath);

        try (Statement statement = connection.createStatement()) {
            statement.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
                """);
        }
    }

    public synchronized long createJob(
            String name, int priority) throws SQLException {

        String sql = """
            INSERT INTO jobs (name, priority, status)
            VALUES (?, ?, 'QUEUED')
            """;

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             sql, Statement.RETURN_GENERATED_KEYS)) {

            statement.setString(1, name);
            statement.setInt(2, priority);
            statement.executeUpdate();

            try (ResultSet keys = statement.getGeneratedKeys()) {
                if (keys.next()) {
                    return keys.getLong(1);
                }
            }
        }

        throw new SQLException("Could not create job record");
    }

    public synchronized void updateStatus(
            long id, String status, String errorMessage)
            throws SQLException {

        String sql = """
            UPDATE jobs
            SET status = ?, error_message = ?
            WHERE id = ?
            """;

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            statement.setString(1, status);
            statement.setString(2, errorMessage);
            statement.setLong(3, id);

            if (statement.executeUpdate() != 1) {
                throw new SQLException("Job not found: " + id);
            }
        }
    }

    public synchronized Optional<String> findStatus(long id)
            throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "SELECT status FROM jobs WHERE id = ?")) {
            statement.setLong(1, id);

            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? Optional.of(result.getString("status"))
                        : Optional.empty();
            }
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }
}
