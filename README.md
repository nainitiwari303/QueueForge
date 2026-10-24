# QueueForge

**A priority-based background job scheduler built with Java 21, concurrent worker threads, and SQLite persistence.**

QueueForge is a backend engineering project that schedules jobs by priority, executes them using a configurable worker pool, tracks their lifecycle, and stores job metadata and statuses in a SQLite database.

It demonstrates practical Java concurrency, thread coordination, database integration, failure handling, and automated testing.

## Features

- **Priority-based scheduling:** Higher-priority jobs are selected before lower-priority jobs that are still waiting in the queue.
- **Concurrent execution:** A configurable pool of worker threads processes jobs.
- **Deterministic tie-breaking:** Jobs with equal priority are ordered by submission sequence while pending.
- **Job lifecycle tracking:** Tracks states such as `QUEUED`, `RUNNING`, `COMPLETED`, and `FAILED`.
- **SQLite persistence:** Stores job metadata, statuses, and error information.
- **Failure isolation:** A failed job does not inherently prevent workers from processing other jobs.
- **Graceful shutdown:** Supports orderly shutdown and processing of accepted work.
- **Automated testing:** JUnit tests cover scheduling, failure handling, shutdown, and database persistence.

## Technology Stack

- Java 21
- Maven
- SQLite
- SQLite JDBC
- JUnit 5

## Project Structure

```text
QueueForge/
├── src/
│   ├── main/java/com/queueforge/
│   │   ├── QueueForge.java
│   │   └── JobStore.java
│   └── test/java/com/queueforge/
│       ├── QueueForgeTest.java
│       ├── JobStoreTest.java
│       └── DatabaseInspector.java
├── .gitignore
├── mvnw
├── mvnw.cmd
├── pom.xml
└── README.md
```

## Requirements

- Java Development Kit (JDK) 21 or later
- Git (optional, for cloning the repository)

## Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/nainitiwari303/QueueForge.git
cd QueueForge
```

### 2. Run the automated tests

On Windows:

```powershell
.\mvnw.cmd test
```

On macOS or Linux:

```bash
./mvnw test
```

Maven downloads the required dependencies and runs the JUnit test suite.

### 3. Run the scheduler demo

```powershell
.\mvnw.cmd org.codehaus.mojo:exec-maven-plugin:3.6.4:java "-Dexec.mainClass=com.queueforge.QueueForge"
```

Run this command from the project root. It uses the Maven Exec plugin to launch the scheduler's main class.

## How It Works

1. A job is submitted with a priority and task.
2. The scheduler places the job in a priority queue.
3. An available worker takes a pending job and executes its task.
4. Job status and relevant metadata are recorded in SQLite.
5. The job finishes with a `COMPLETED` or `FAILED` status.
6. The scheduler can shut down gracefully when processing is finished.

**Concurrency note:** Priority determines which pending jobs are selected first. With multiple workers, actual execution start and completion order can differ because jobs run concurrently.

## Current Scope and Limitations

QueueForge is currently a runnable local Java application, not a hosted web service.

- SQLite stores job metadata and status; executable task logic is not persisted.
- Automatic recovery and re-execution of unfinished jobs after a process crash are not implemented.
- Retry policies, idempotency safeguards, a REST API, and a web monitoring dashboard are not currently implemented.
- Performance benchmarks have not yet been established.

## Future Improvements

- Configurable retry policies and exponential backoff
- Idempotent job execution
- Recovery strategies for interrupted jobs
- REST API for job submission and status queries
- Monitoring dashboard and metrics
- Load testing and performance benchmarks

## Author

**Naini Tiwari**

GitHub: [@nainitiwari303](https://github.com/nainitiwari303)
