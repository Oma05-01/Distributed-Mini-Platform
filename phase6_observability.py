"""
Phase 6 — Observability (Tracing, Metrics, Structured Logging)
==============================================================
The final architecture. We introduce a central Logger, Request IDs
for distributed tracing, and an updated metrics dashboard.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    CHAOS ON | OFF
    METRICS         <-- Upgraded from STATS
    EXIT

Run:
    python phase6_observability.py
"""

import json
import os
import time
import random
import threading
import uuid
from typing import Optional, Any

DB_FILE = "store_db.json"

class SimulatedNetworkError(Exception): pass
class CircuitBreakerOpenError(Exception): pass

# ---------------------------------------------------------------------------
# Observability Layer (NEW)
# ---------------------------------------------------------------------------
class Logger:
    """Centralized structured logging."""
    @staticmethod
    def log(service: str, req_id: str, level: str, message: str):
        timestamp = time.strftime('%H:%M:%S')
        # Format: [TIME] [SERVICE] [REQ_ID] [LEVEL] Message
        print(f"[{timestamp}] [{service:^7}] [{req_id:^8}] {level:^4} | {message}")

class MetricsCollector:
    """Tracks system health and performance."""
    def __init__(self):
        self.total_requests = 0
        self.failed_requests = 0
        self.total_latency_ms = 0.0

    def record_request(self, latency_sec: float, success: bool):
        self.total_requests += 1
        self.total_latency_ms += (latency_sec * 1000)
        if not success:
            self.failed_requests += 1

    def get_avg_latency(self) -> float:
        if self.total_requests == 0: return 0.0
        return self.total_latency_ms / self.total_requests

# ---------------------------------------------------------------------------
# Resilience Patterns (From Phase 5)
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, max_requests: int = 5, window_seconds: int = 10):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests = []

    def allow(self) -> bool:
        now = time.time()
        self.requests = [req for req in self.requests if now - req < self.window]
        if len(self.requests) >= self.max_requests: return False
        self.requests.append(now)
        return True

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 5.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = 0.0

    def call(self, func, *args, **kwargs) -> Any:
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                Logger.log("BREAKER", "SYSTEM", "WARN", "HALF-OPEN: Testing if service recovered...")
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit is OPEN. Fast failing request.")
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                Logger.log("BREAKER", "SYSTEM", "INFO", "CLOSED: Service recovered!")
                self.state = "CLOSED"
                self.failures = 0
            return result
        except SimulatedNetworkError as e:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold and self.state != "OPEN":
                Logger.log("BREAKER", "SYSTEM", "ERR", f"OPEN: Threshold reached ({self.failures} failures). Tripping circuit!")
                self.state = "OPEN"
            raise e

# ---------------------------------------------------------------------------
# Core Services (Updated with req_id tracing)
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, path: str = DB_FILE) -> None:
        self.path = path
        self._data: dict[str, str] = self._load()
        self.chaos_mode = False

    def _load(self) -> dict[str, str]:
        if not os.path.exists(self.path): return {}
        with open(self.path, "r", encoding="utf-8") as f: return json.load(f)

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f: json.dump(self._data, f, indent=2)

    def _maybe_fail(self, req_id: str):
        if self.chaos_mode and random.random() < 0.4:
            Logger.log("DB", req_id, "ERR", "NETWORK ERROR: Connection dropped!")
            raise SimulatedNetworkError("DB Connection Timeout")
        time.sleep(0.5)

    def get(self, key: str, req_id: str) -> Optional[str]:
        self._maybe_fail(req_id)
        Logger.log("DB", req_id, "INFO", f"READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str, req_id: str) -> None:
        self._maybe_fail(req_id)
        Logger.log("DB", req_id, "INFO", f"WRITE -> '{key}': '{value}'")
        self._data[key] = value
        self._flush()

    def delete(self, key: str, req_id: str) -> bool:
        self._maybe_fail(req_id)
        Logger.log("DB", req_id, "INFO", f"DELETE -> '{key}'")
        if key in self._data:
            del self._data[key]
            self._flush()
            return True
        return False

class Cache:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self.chaos_mode = False

    def _maybe_drop(self, req_id: str):
        if self.chaos_mode and random.random() < 0.2:
            Logger.log("CACHE", req_id, "WARN", "KILLED: Cache node restarted, data lost!")
            self._data.clear()

    def get(self, key: str, req_id: str) -> Optional[str]:
        self._maybe_drop(req_id)
        Logger.log("CACHE", req_id, "INFO", f"READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str, req_id: str) -> None:
        Logger.log("CACHE", req_id, "INFO", f"WRITE -> '{key}': '{value}'")
        self._data[key] = value

    def delete(self, key: str, req_id: str) -> None:
        Logger.log("CACHE", req_id, "INFO", f"DELETE -> '{key}'")
        self._data.pop(key, None)

class MessageQueue:
    def __init__(self) -> None:
        self._events: list[dict] = []

    def publish(self, event_type: str, payload: dict, req_id: str) -> None:
        self._events.append({"type": event_type, "payload": payload, "req_id": req_id})
        Logger.log("QUEUE", req_id, "INFO", f"PUBLISHED -> {event_type} | Backlog: {len(self._events)}")

    def consume(self) -> Optional[dict]:
        return self._events.pop(0) if self._events else None

    def size(self) -> int:
        return len(self._events)

class BackgroundWorker:
    def __init__(self, queue: MessageQueue) -> None:
        self.queue = queue
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self): self.thread.start()
    def stop(self): self.running = False

    def _run_loop(self):
        while self.running:
            event = self.queue.consume()
            if event:
                req_id = event["req_id"]
                Logger.log("WORKER", req_id, "INFO", f"⚙️ Processing {event['type']} for '{event['payload'].get('key')}'...")
                time.sleep(2.0)
                Logger.log("WORKER", req_id, "INFO", f"✅ DONE processing '{event['payload'].get('key')}'.")
                print("\nclient ❯ ", end="", flush=True)
            else:
                time.sleep(1.0)

# ---------------------------------------------------------------------------
# API Gateway
# ---------------------------------------------------------------------------
class APIGateway:
    def __init__(self, db: Database, cache: Cache, queue: MessageQueue) -> None:
        self.db = db
        self.cache = cache
        self.queue = queue
        self.rate_limiter = RateLimiter()
        self.db_breaker = CircuitBreaker()
        self.metrics = MetricsCollector()

    def _execute_with_retry(self, func, *args, req_id: str, max_retries=2):
        for attempt in range(max_retries + 1):
            try:
                return self.db_breaker.call(func, *args, req_id=req_id)
            except SimulatedNetworkError:
                if attempt < max_retries:
                    backoff = 2 ** attempt
                    Logger.log("GATEWAY", req_id, "WARN", f"Retry {attempt + 1}/{max_retries} in {backoff}s...")
                    time.sleep(backoff)
                else:
                    Logger.log("GATEWAY", req_id, "ERR", "Exhausted all retries.")
                    raise

    def handle_request(self, command: str) -> str:
        req_id = str(uuid.uuid4())[:8]  # Trace ID generated at the edge!
        start_time = time.time()
        success = False
        response = ""

        if not self.rate_limiter.allow():
            response = "429 TOO MANY REQUESTS"
            self.metrics.record_request(time.time() - start_time, success=False)
            return response

        parts = command.strip().split()
        if not parts: return ""
        action = parts[0].upper()

        Logger.log("GATEWAY", req_id, "INFO", f"Received command: {action}")

        try:
            if action == "GET":
                if len(parts) != 2: raise ValueError("400 BAD REQUEST: GET requires a key")
                key = parts[1]

                value = self.cache.get(key, req_id)
                if value is not None:
                    Logger.log("GATEWAY", req_id, "INFO", "CACHE HIT ⚡")
                    response = f"200 OK: {value}"
                else:
                    Logger.log("GATEWAY", req_id, "INFO", "CACHE MISS 🐌")
                    value = self._execute_with_retry(self.db.get, key, req_id=req_id)

                    if value is None:
                        response = f"404 NOT FOUND: '{key}'"
                    else:
                        self.cache.set(key, value, req_id)
                        response = f"200 OK: {value}"
                success = True

            elif action == "SET":
                if len(parts) < 3: raise ValueError("400 BAD REQUEST: SET requires key and value")
                key = parts[1]
                value = " ".join(parts[2:])

                self._execute_with_retry(self.db.set, key, value, req_id=req_id)
                self.cache.delete(key, req_id)
                self.queue.publish("KEY_UPDATED", {"key": key, "value": value}, req_id)
                response = "201 CREATED"
                success = True

            elif action == "DELETE":
                if len(parts) != 2: raise ValueError("400 BAD REQUEST: DELETE requires a key")
                key = parts[1]

                if self._execute_with_retry(self.db.delete, key, req_id=req_id):
                    self.cache.delete(key, req_id)
                    self.queue.publish("KEY_DELETED", {"key": key}, req_id)
                    response = "200 OK: Deleted"
                else:
                    response = f"404 NOT FOUND: '{key}'"
                success = True

            elif action == "CHAOS":
                if len(parts) == 2 and parts[1].upper() in ["ON", "OFF"]:
                    mode = parts[1].upper() == "ON"
                    self.db.chaos_mode = mode
                    self.cache.chaos_mode = mode
                    response = f"200 OK: Chaos mode is now {'🔥 ON' if mode else '🛑 OFF'}"
                    success = True
                else:
                    raise ValueError("400 BAD REQUEST: Use CHAOS ON or CHAOS OFF")

            elif action == "METRICS":
                stats = (
                    f"\n    [Metrics Dashboard]\n"
                    f"    Total Requests:  {self.metrics.total_requests}\n"
                    f"    Failed Requests: {self.metrics.failed_requests}\n"
                    f"    Error Rate:      {(self.metrics.failed_requests / max(1, self.metrics.total_requests))*100:.1f}%\n"
                    f"    Avg Latency:     {self.metrics.get_avg_latency():.1f} ms\n"
                    f"    Queue Backlog:   {self.queue.size()}\n"
                    f"    DB Breaker:      {self.db_breaker.state}"
                )
                response = f"200 OK: {stats}"
                success = True

            else:
                raise ValueError(f"400 BAD REQUEST: Unknown command '{action}'")

        except CircuitBreakerOpenError as e:
            Logger.log("GATEWAY", req_id, "ERR", "Failed fast due to open circuit.")
            response = f"503 SERVICE UNAVAILABLE: {str(e)}"
        except SimulatedNetworkError:
            Logger.log("GATEWAY", req_id, "ERR", "Operation failed after retries.")
            response = "500 INTERNAL SERVER ERROR: Database connection failed."
        except ValueError as e:
            response = str(e)

        # Record metrics at the end of the request
        latency = time.time() - start_time
        self.metrics.record_request(latency, success)
        Logger.log("GATEWAY", req_id, "INFO", f"Request completed in {latency*1000:.1f}ms")
        return response

# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def main():
    print("=" * 80)
    print("Distributed Platform — Phase 6 (Observability & Integration)")
    print("=" * 80)

    db = Database()
    cache = Cache()
    queue = MessageQueue()
    worker = BackgroundWorker(queue)
    worker.start()

    api = APIGateway(db, cache, queue)

    print("\nSystem ready. Commands: GET, SET, DELETE, CHAOS ON/OFF, METRICS, EXIT.")
    print("Watch the [REQ_ID] to trace how a single request flows through the services!")
    print("─" * 80)

    while True:
        try:
            line = input("\nclient ❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            worker.stop()
            break

        if not line: continue
        if line.upper() == "EXIT":
            worker.stop()
            break

        response = api.handle_request(line)
        print(f"server ❮ {response}")

if __name__ == "__main__":
    main()