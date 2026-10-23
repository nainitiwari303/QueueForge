package com.queueforge;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueueForge {

    private static final AtomicLong SEQUENCE = new AtomicLong();

    record Job(String name, int priority, Runnable task, long sequence)
            implements Comparable<Job> {

        @Override
        public int compareTo(Job other) {
            int priorityOrder =
                    Integer.compare(other.priority(), this.priority());

            if (priorityOrder != 0) {
                return priorityOrder;
            }

            return Long.compare(this.sequence(), other.sequence());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int workerCount = 2;

        PriorityBlockingQueue<Job> queue = new PriorityBlockingQueue<>();

        queue.add(new Job("Generate report", 1,
                () -> process("Generate report"), SEQUENCE.getAndIncrement()));

        queue.add(new Job("Process payment", 10,
                () -> process("Process payment"), SEQUENCE.getAndIncrement()));

        queue.add(new Job("Send notification", 3,
                () -> process("Send notification"), SEQUENCE.getAndIncrement()));

        queue.add(new Job("Validate transaction", 8,
                () -> process("Validate transaction"), SEQUENCE.getAndIncrement()));

        ExecutorService workers = Executors.newFixedThreadPool(workerCount);

        System.out.println("QueueForge started with " + workerCount + " workers.");

        try {
            while (!queue.isEmpty()) {
                Job job = queue.poll();

                if (job != null) {
                    workers.submit(() -> {
                        System.out.printf(
                                "[%s] Executing: %s (priority=%d)%n",
                                Thread.currentThread().getName(),
                                job.name(),
                                job.priority()
                        );

                        job.task().run();
                    });
                }
            }
        } finally {
            workers.shutdown();

            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        }

        System.out.println("QueueForge finished processing all submitted jobs.");
    }

    private static void process(String name) {
        try {
            Thread.sleep(800);
            System.out.println("Completed: " + name);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted: " + name);
        }
    }
}
