
package com.queueforge;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JobStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void createsJobAndStoresItsInitialStatus() throws SQLException {
        Path database = tempDir.resolve("jobs.db");

        try (JobStore store = new JobStore(database.toString())) {
            long id = store.createJob("Generate report", 5);

            assertTrue(id > 0);
            assertEquals(Optional.of("QUEUED"), store.findStatus(id));
        }
    }

    @Test
    void statusSurvivesClosingAndReopeningDatabase()
            throws SQLException {
        Path database = tempDir.resolve("persistent-jobs.db");
        long id;

        try (JobStore store = new JobStore(database.toString())) {
            id = store.createJob("Process payment", 10);
            store.updateStatus(id, "COMPLETED", null);
        }

        try (JobStore reopened = new JobStore(database.toString())) {
            assertEquals(
                    Optional.of("COMPLETED"),
                    reopened.findStatus(id));
        }
    }

    @Test
    void unknownJobHasNoStatus() throws SQLException {
        Path database = tempDir.resolve("empty-jobs.db");

        try (JobStore store = new JobStore(database.toString())) {
            assertEquals(Optional.empty(), store.findStatus(9999));
        }
    }
}
