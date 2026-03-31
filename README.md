## Distributed-Mini-Platform
A micro-distributed system simulator designed to explore the complex interactions, failure modes, and performance trade-offs between decoupled services. This project moves beyond single-component logic to demonstrate how an API, Cache, Database, and Message Queue operate as a unified architecture.

**Why This Project Exists**
Building individual components like a database or a cache is only half the battle. In production, the real difficulty lies in the "glue" between them—how data flows, how state stays consistent, and how the system behaves when one part inevitably fails.


This project was built to understand:

* Service Orchestration: How an API Gateway manages traffic between fast volatile storage and slow persistent storage.

* Asynchronous Decoupling: How to use background workers to offload heavy tasks from the main user-facing thread.

* Resilience Engineering: Implementing patterns like Circuit Breakers and Retries to prevent cascading failures.

* Observability: Using distributed tracing (Request IDs) to follow a single action through multiple independent services.

## Scope & Constraints
**Included**

* API Gateway: A central command-loop acting as the system's entry point.
* Cache-Aside Pattern: In-memory caching with lazy population and write-invalidation.
* Persistent Database: JSON-backed storage with simulated I/O latency.
* Asynchronous Queue: A thread-safe message queue for background event publishing.
* Threaded Worker: A background process that consumes events and executes simulated tasks.
* Resilience Suite: Functional Rate Limiting, Circuit Breakers, and Exponential Backoff Retries.
* Observability: Structured logging and Request ID tracing across all services.

**Explicitly Excluded**
* Network Protocols: All services communicate via shared memory/method calls rather than TCP/HTTP to focus on logic over networking.
* Containerization: No Docker or Kubernetes; the system runs as a single multi-threaded process.
* Distributed Consensus: No Raft or Paxos; the system assumes a single source of truth for simplicity.
* Authentication: No user sessions or permissions logic.

## Architecture Overview
Plaintext
            [ Client Interface ]
                     │
                     ▼
              [ API Gateway ] ───────┐
                     │               │
       ┌─────────────┴─────────────┐ │
       ▼                           ▼ │ (Async Events)
   [ Cache ] <───(Sync)───> [ Database ] │
 (In-Memory)                (JSON File)  │
                                     │   ▼
                                [ Message Queue ]
                                     │
                                     ▼
                            [ Background Worker ]
                             (Email/Analytics)

## Core Components
**API Gateway**
The system's "Brain." It handles rate limiting, generates unique Request IDs, and orchestrates the flow of data between the Cache and DB.

**Cache Layer**
A high-speed, volatile storage layer. It reduces DB load by storing frequently accessed data, implementing a 20% random "node crash" simulation in Chaos Mode.

**Database Layer**
The "Source of Truth." It persists data to disk and includes a 40% "Network Error" simulation to test system resilience.

**Background Worker**
A daemon thread that monitors the Message Queue. It simulates "heavy" tasks like sending emails or updating analytics, taking 2 seconds per task without slowing down the API.

## Key Design Decisions
**1 — Request ID Tracing**
Every request is assigned a unique 8-character ID at the gateway. This ID is passed to every downstream service, including the asynchronous worker.

* Tradeoff: Adds slight metadata overhead but makes debugging distributed failures possible.

**2 — Circuit Breaker Logic**
The DB is wrapped in a breaker that "trips" after 3 consecutive failures.

* Tradeoff: Protects the DB from being hammered while it’s struggling, but results in "Fast Fails" for the user until the recovery timeout expires.

**3 — Cache-Aside vs. Write-Through**
We chose Cache-Aside (Lazy Population).

* Tradeoff: Simplifies the write path (just invalidate the cache), but the first read after a write will always be slow (Cache Miss).

## Performance Characteristics
**Read Latency**
* Cache Hit: <1ms (Instant response).
* Cache Miss: ~500ms (Due to simulated DB disk I/O).

**Write Throughput**


**API Response:** Fast. The Gateway returns as soon as the DB write is confirmed, offloading secondary work to the Queue.

**Worker Throughput:** 0.5 tasks/second. The worker is intentionally slower than the API to demonstrate queue backlogs.

## Failure Modes & Limitations
* **Stale Data:** In a true distributed system, there is a tiny window where the Cache might return old data before it is invalidated.

* **Queue Loss:** Since the queue is in-memory, if the main process crashes, all unprocessed background tasks are lost.

* **Single Threaded API:** While the worker is threaded, the API Gateway processes one CLI command at a time.

## System Experiments

**The Chaos Test**

**Test:** Enabled CHAOS ON and bombarded the system with 10 rapid SET requests.

**Observation:** The Rate Limiter blocked the flood. The requests that got through hit DB errors. The Gateway retried them. Eventually, the Circuit Breaker tripped, and the system began "failing fast" to save itself.

## Distributed Trace Test
**Test:** Followed Request ID: a1b2c3d4.


**Observation:** Saw the ID logged by the Gateway, then the DB, then the Queue. Two seconds after the API responded, the Worker logged the same ID while processing the task.


**Insight:** Proved the decoupling of the user experience from the system's background work.

## How to Run
1. Ensure store_db.json exists in the directory (or the script will create it).

2. Run the final phase:

```Bash
python phase6_observability.py
```

3. Commands to try:
```
* SET user:101 Atirolaoluwa

* GET user:101 (First one is slow, second is fast!)

* CHAOS ON

* METRICS
```