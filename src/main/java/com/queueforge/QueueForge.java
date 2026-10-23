package com.queueforge;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueueForge implements AutoCloseable {

    private static final AtomicLong SEQUENCE = new AtomicLong();
    private static final String STOP = "__STOP__";

    record Job(String name, int priority, Runnable task, long sequence)
            implements Comparable<Job> {

        @Override
        public int compareTo(Job other) {
            int result = Integer.compare(other.priority, priority);
            return result != 0
                    ? result
                    : Long.compare(sequence, other.sequence);
        }
    }

    private final PriorityBlockingQueue<Job> queue =
            new PriorityBlockingQueue<>();

    private final ExecutorService workers;
    private final int workerCount;

    public QueueForge(int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException(
                    "workerCount must be at least 1");
        }

        this.workerCount = workerCount;
        this.workers = Executors.newFixedThreadPool(workerCount);

        for (int i = 0; i < workerCount; i++) {
            workers.submit(this::workerLoop);
        }
    }

    public void submit(String name, int priority, Runnable task) {
        if (workers.isShutdown()) {
            throw new IllegalStateException("Scheduler is shutting down");
        }

        queue.add(new Job(
                name, priority, task, SEQUENCE.getAndIncrement()));
    }

    private void workerLoop() {
        try {
            while (true) {
                Job job = queue.take();

                if (STOP.equals(job.name())) {
                    return;
                }

                try {
                    System.out.printf(
                            "[%s] Started: %s (priority=%d)%n",
                            Thread.currentThread().getName(),
                            job.name(),
                            job.priority());

                    job.task().run();

                    System.out.println("Completed: " + job.name());
                } catch (Exception e) {
                    System.err.printf(
                            "Job failed: %s — %s%n",
                            job.name(), e.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        for (int i = 0; i < workerCount; i++) {
            queue.add(new Job(
                    STOP, Integer.MIN_VALUE, () -> {},
                    SEQUENCE.getAndIncrement()));
        }

        workers.shutdown();

        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
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
