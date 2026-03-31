## System Postmortem: Failure Stories
**Project: Distributed Mini Platform**
**Date: March 2026**

This document records the simulated failures injected during Phase 5 and how the resilience patterns (Circuit Breaker, Rate Limiter, Retries) protected the system from cascading collapse.

## Incident 1: The Database Goes Dark
**The Scenario:** Chaos mode was enabled, simulating a 40% packet drop rate to the primary database. A client sent a burst of SET requests.

**What Happened:**

- The first write failed. The API Gateway logged [GATEWAY] ⚠️ Retry 1/2 in 1s... and successfully completed the write on the second attempt.
  
- As the burst continued, the DB failed three times in rapid succession.
  
- The Circuit Breaker tripped: [BREAKER] 🔴 OPEN: Threshold reached (3 failures).
  
- Subsequent requests immediately returned 503 SERVICE UNAVAILABLE without attempting to contact the DB.


**The System's Defense:** The Circuit Breaker worked exactly as designed. By "failing fast," the API Gateway prevented worker threads from hanging indefinitely while waiting for a dead database, which in a real system would exhaust connection pools and take down the API entirely.

## Incident 2: The Cache Amnesia
**The Scenario:** A simulated node reboot wiped the entire in-memory Cache layer while read traffic was steady.

**What Happened:**

- A previously "hot" key (e.g., user-1) was requested via GET user-1.
  
- Instead of a CACHE HIT ⚡, the Gateway registered a CACHE MISS 🐌.
  
- The Gateway fell back to the Database, retrieved the value (incurring the 0.5s I/O penalty), and synchronously repopulated the Cache.


**The System's Defense:** The Cache-Aside pattern proved its worth. The system never returned an error; it gracefully degraded. The only symptom experienced by the client was a temporary 500ms latency spike on the first read, after which normal <10ms response times resumed.

## Incident 3: The Throttled Client
**The Scenario:** A script attempted to flood the system with 20 GET requests in under 5 seconds.

**What Happened:**

- The first 5 requests processed normally.
  
- On the 6th request, the Rate Limiter evaluated the rolling 10-second window and blocked the call.
  
- The API Gateway returned 429 TOO MANY REQUESTS.


**The System's Defense:** This prevented a "noisy neighbor" from monopolizing system resources. In a real environment, this protects the DB and Queue from being overwhelmed by a DDoS attack or a misconfigured client script.

## Incident 4: The Overwhelmed Worker
**The Scenario**: A batch of 10 SET commands was sent successfully. The API Gateway responded instantly (201 CREATED).

**What Happened:**

- The Queue backlog spiked to 10.
  
- Because the Background Worker takes 2 seconds to process each event, it took 20 seconds to drain the queue.
  
- The client continued to read and write without any latency degradation.


**The System's Defense:** Asynchronous decoupling saved the API. If the API had waited for the worker's tasks (like sending emails) to finish synchronously, a single SET would have taken 2.5 seconds, and 10 concurrent sets would have completely locked the system.
