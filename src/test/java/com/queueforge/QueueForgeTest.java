package com.queueforge;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueueForgeTest {

    @Test
    void higherPriorityJobComesFirst() {
        QueueForge.Job low =
                new QueueForge.Job("Low", 1, () -> {}, 0);

        QueueForge.Job high =
                new QueueForge.Job("High", 10, () -> {}, 1);

        assertTrue(high.compareTo(low) < 0);
    }

    @Test
    void equalPriorityJobsPreserveSequenceOrder() {
        QueueForge.Job earlier =
                new QueueForge.Job("Earlier", 5, () -> {}, 1);

        QueueForge.Job later =
                new QueueForge.Job("Later", 5, () -> {}, 2);

        assertTrue(earlier.compareTo(later) < 0);
    }
}