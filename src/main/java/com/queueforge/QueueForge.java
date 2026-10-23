

package com.queueforge;

import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueueForge implements AutoCloseable {

    private static final AtomicLong SEQUENCE = new AtomicLong();
    private static final String STOP = "__QUEUEFORGE_STOP__";

    public record Job(
            long id,
            String name,
            int priority,
            Runnable task,
            long sequence
    ) implements Comparable<Job> {

        @Override
        public int compareTo(Job other) {
            int result = Integer.compare(other.priority(), priority());
            return result != 0
                    ? result
                    : Long.compare(sequence, other.sequence());
        }
    }

    private final PriorityBlockingQueue<Job> queue =
            new PriorityBlockingQueue<>();

    private final ExecutorService workers;
    private final int workerCount;
    private final JobStore store;

    private boolean accepting = true;

    public QueueForge(int workerCount) {
        this(workerCount, "queueforge.db");
    }

    public QueueForge(int workerCount, String databasePath) {
        if (workerCount < 1) {
            throw new IllegalArgumentException(
                    "workerCount must be at least 1");
        }

        this.workerCount = workerCount;

        try {
            this.store = new JobStore(databasePath);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Could not initialize job database", e);
        }

        this.workers = Executors.newFixedThreadPool(workerCount);

        for (int i = 0; i < workerCount; i++) {
            workers.submit(this::workerLoop);
        }
    }

    public synchronized long submit(
            String name, int priority, Runnable task) {

        if (!accepting) {
            throw new IllegalStateException(
                    "QueueForge is shutting down");
        }

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Job name must not be blank");
        }

        if (task == null) {
            throw new IllegalArgumentException(
                    "Job task must not be null");
        }

        final long id;

        try {
            id = store.createJob(name, priority);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Could not persist submitted job", e);
        }

        queue.add(new Job(
                id, name, priority, task, SEQUENCE.getAndIncrement()));

        return id;
    }

    private void workerLoop() {
        try {
            while (true) {
                Job job = queue.take();

                if (STOP.equals(job.name())) {
                    return;
                }

                try {
                    updateStatus(job.id(), "RUNNING", null);

                    System.out.printf(
                            "[%s] Started: %s (priority=%d, id=%d)%n",
                            Thread.currentThread().getName(),
                            job.name(),
                            job.priority(),
                            job.id());

                    job.task().run();

                    updateStatus(job.id(), "COMPLETED", null);
                    System.out.println("Completed: " + job.name());

                } catch (RuntimeException e) {
                    markFailed(job.id(), e.getMessage());

                    System.err.printf(
                            "Job failed: %s — %s%n",
                            job.name(), e.getMessage());

                              }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void updateStatus(
            long id, String status, String errorMessage) {
        try {
            store.updateStatus(id, status, errorMessage);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Could not update job " + id
                            + " to " + status, e);
        }
    }

    private void markFailed(long id, String message) {
        try {
            store.updateStatus(id, "FAILED", message);
        } catch (SQLException e) {
            System.err.println(
                    "Could not persist failure for job "
                            + id + ": " + e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        if (!accepting) {
            return;
        }

        accepting = false;

        for (int i = 0; i < workerCount; i++) {
            queue.add(new Job(
                    -1,
                    STOP,
                    Integer.MIN_VALUE,
                    () -> {},
                    SEQUENCE.getAndIncrement()));
        }

        workers.shutdown();

        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                workers.shutdownNow();
                workers.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            try {
                store.close();
            } catch (SQLException e) {
                System.err.println(
                        "Could not close job database: "
                                + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        try (QueueForge scheduler = new QueueForge(2)) {
            scheduler.submit("Generate report", 1,
                    () -> process("Generate report"));

            scheduler.submit("Process payment", 10,
                    () -> process("Process payment"));

            scheduler.submit("Send notification", 3,
                    () -> process("Send notification"));

            scheduler.submit("Validate transaction", 8,
                    () -> process("Validate transaction"));
        }

        System.out.println("QueueForge shut down cleanly.");
    }

    private static void process(String name) {
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }
}

