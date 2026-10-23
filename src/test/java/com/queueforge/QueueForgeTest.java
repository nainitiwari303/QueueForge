

package com.queueforge;

import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class QueueForgeTest {

    @Test
    void higherPriorityJobComesFirst() {
        QueueForge.Job low =
                new QueueForge.Job(1, "Low", 1, () -> {}, 0);

        QueueForge.Job high =
                new QueueForge.Job(2, "High", 10, () -> {}, 1);

        assertTrue(high.compareTo(low) < 0);
    }

    @Test
    void equalPriorityJobsPreserveSequenceOrder() {
        QueueForge.Job earlier =
                new QueueForge.Job(1, "Earlier", 5, () -> {}, 1);

        QueueForge.Job later =
                new QueueForge.Job(2, "Later", 5, () -> {}, 2);

        assertTrue(earlier.compareTo(later) < 0);
    }

    @Test
    void failedJobDoesNotStopOtherJobs() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);

        try (QueueForge scheduler = new QueueForge(1)) {
            scheduler.submit("Failing job", 10, () -> {
                throw new RuntimeException("Expected test failure");
            });

            scheduler.submit("Healthy job", 5, completed::countDown);

            assertTrue(completed.await(3, TimeUnit.SECONDS),
                    "Healthy job should run after a job fails");
        }
    }

    @Test
    void closeDrainsAcceptedJobsAndRejectsNewSubmissions()
            throws Exception {

        int totalJobs = 10;
        CountDownLatch completed = new CountDownLatch(totalJobs);
        AtomicInteger completedCount = new AtomicInteger();

        QueueForge scheduler = new QueueForge(2);

        for (int i = 0; i < totalJobs; i++) {
            scheduler.submit("Job-" + i, 1, () -> {
                completedCount.incrementAndGet();
                completed.countDown();
            });
        }

        scheduler.close();

        assertTrue(completed.await(1, TimeUnit.SECONDS));
        assertEquals(totalJobs, completedCount.get());

        assertThrows(IllegalStateException.class,
                () -> scheduler.submit("Too late", 1, () -> {}));
    }
}
