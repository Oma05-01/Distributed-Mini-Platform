"""
Phase 5 — Failure Simulation & Resilience
=========================================
We introduce Chaos Mode to simulate network drops and cache evictions.
To handle this, we wrap our Database calls in a Circuit Breaker and add
Retry logic to the API Gateway. We also add a Rate Limiter to protect the API.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    CHAOS ON | OFF  <-- Toggle failure injection
    STATS
    EXIT

Run:
    python phase5_failures_resilience.py
"""

import json
import os
import time
import random
import threading
from typing import Optional, Any

DB_FILE = "store_db.json"


class SimulatedNetworkError(Exception):
    pass


class CircuitBreakerOpenError(Exception):
    pass


# ---------------------------------------------------------------------------
# Resilience Patterns
# ---------------------------------------------------------------------------
class RateLimiter:
    """Token-bucket style rate limiter (e.g., max 5 requests per 10 seconds)."""

    def __init__(self, max_requests: int = 5, window_seconds: int = 10):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests = []

    def allow(self) -> bool:
        now = time.time()
        # Remove requests older than the window
        self.requests = [req for req in self.requests if now - req < self.window]
        if len(self.requests) >= self.max_requests:
            return False
        self.requests.append(now)
        return True


class CircuitBreaker:
    """
    Protects a failing service.
    CLOSED -> Normal operation.
    OPEN -> Fails fast without calling the service (protects the DB).
    HALF-OPEN -> Allows a test request through after a timeout.
    """

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 5.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = 0.0

    def call(self, func, *args, **kwargs) -> Any:
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                print("    [CircuitBreaker] 🟡 HALF-OPEN: Testing if service recovered...")
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit is OPEN. Fast failing request.")

        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                print("    [CircuitBreaker] 🟢 CLOSED: Service recovered!")
                self.state = "CLOSED"
                self.failures = 0
            return result
        except SimulatedNetworkError as e:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold and self.state != "OPEN":
                print(f"    [CircuitBreaker] 🔴 OPEN: Threshold reached ({self.failures} failures). Tripping circuit!")
                self.state = "OPEN"
            raise e


# ---------------------------------------------------------------------------
# Service 1: Database Layer (Now with Chaos!)
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, path: str = DB_FILE) -> None:
        self.path = path
        self._data: dict[str, str] = self._load()
        self.chaos_mode = False
        print("  [System] Database service initialized.")

    def _load(self) -> dict[str, str]:
        if not os.path.exists(self.path): return {}
        with open(self.path, "r", encoding="utf-8") as f: return json.load(f)

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f: json.dump(self._data, f, indent=2)

    def _maybe_fail(self):
        if self.chaos_mode and random.random() < 0.4:  # 40% chance to fail
            print("    [DB] 💥 NETWORK ERROR: Connection dropped!")
            raise SimulatedNetworkError("DB Connection Timeout")
        time.sleep(0.5)  # Normal latency

    def get(self, key: str) -> Optional[str]:
        self._maybe_fail()
        print(f"    [DB] READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        self._maybe_fail()
        print(f"    [DB] WRITE -> '{key}': '{value}'")
        self._data[key] = value
        self._flush()

    def delete(self, key: str) -> bool:
        self._maybe_fail()
        print(f"    [DB] DELETE -> '{key}'")
        if key in self._data:
            del self._data[key]
            self._flush()
            return True
        return False


# ---------------------------------------------------------------------------
# Service 2: Cache Layer (Now with random evictions)
# ---------------------------------------------------------------------------
class Cache:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self.chaos_mode = False
        print("  [System] Cache service initialized.")

    def _maybe_drop(self):
        if self.chaos_mode and random.random() < 0.2:  # 20% chance cache clears itself
            print("    [Cache] 💥 KILLED: Cache node restarted, data lost!")
            self._data.clear()

    def get(self, key: str) -> Optional[str]:
        self._maybe_drop()
        print(f"    [Cache] READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        print(f"    [Cache] WRITE -> '{key}': '{value}'")
        self._data[key] = value

    def delete(self, key: str) -> None:
        print(f"    [Cache] DELETE -> '{key}'")
        self._data.pop(key, None)


# ---------------------------------------------------------------------------
# Service 3 & 4: Queue and Worker (Same as Phase 4)
# ---------------------------------------------------------------------------
class MessageQueue:
    def __init__(self) -> None:
        self._events: list[dict] = []

    def publish(self, event_type: str, payload: dict) -> None:
        self._events.append({"type": event_type, "payload": payload})
        print(f"    [Queue] PUBLISHED -> {event_type} | Backlog: {len(self._events)}")

    def consume(self) -> Optional[dict]:
        return self._events.pop(0) if self._events else None

    def size(self) -> int:
        return len(self._events)


class BackgroundWorker:
    def __init__(self, queue: MessageQueue) -> None:
        self.queue = queue
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.running = False

    def _run_loop(self):
        while self.running:
            event = self.queue.consume()
            if event:
                print(f"\n    [Worker] ⚙️ Processing {event['type']} for '{event['payload'].get('key')}'...")
                time.sleep(2.0)
                print(f"    [Worker] ✅ DONE processing '{event['payload'].get('key')}'.")
                print("\nclient ❯ ", end="", flush=True)
            else:
                time.sleep(1.0)


# ---------------------------------------------------------------------------
# Service 5: API Gateway (Now with Retries and Rate Limiting)
# ---------------------------------------------------------------------------
class APIGateway:
    def __init__(self, db: Database, cache: Cache, queue: MessageQueue) -> None:
        self.db = db
        self.cache = cache
        self.queue = queue
        self.rate_limiter = RateLimiter(max_requests=5, window_seconds=10)
        self.db_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5.0)
        print("  [System] API Gateway initialized with Resilience Patterns.")

    def _execute_with_retry(self, func, *args, max_retries=2):
        """Attempts an operation. If network fails, backs off and retries."""
        for attempt in range(max_retries + 1):
            try:
                # We call the DB through the circuit breaker!
                return self.db_breaker.call(func, *args)
            except SimulatedNetworkError:
                if attempt < max_retries:
                    backoff = 2 ** attempt
                    print(f"    [Gateway] ⚠️ Retry {attempt + 1}/{max_retries} in {backoff}s...")
                    time.sleep(backoff)
                else:
                    print("    [Gateway] ❌ Exhausted all retries.")
                    raise

    def handle_request(self, command: str) -> str:
        if not self.rate_limiter.allow():
            return "429 TOO MANY REQUESTS: Please slow down."

        parts = command.strip().split()
        if not parts: return ""
        action = parts[0].upper()

        try:
            if action == "GET":
                if len(parts) != 2: return "400 BAD REQUEST"
                key = parts[1]

                value = self.cache.get(key)
                if value is not None:
                    print("    [Gateway] CACHE HIT ⚡")
                    return f"200 OK: {value}"

                print("    [Gateway] CACHE MISS 🐌")
                value = self._execute_with_retry(self.db.get, key)

                if value is None:
                    return f"404 NOT FOUND: '{key}'"

                self.cache.set(key, value)
                return f"200 OK: {value}"

            elif action == "SET":
                if len(parts) < 3: return "400 BAD REQUEST"
                key = parts[1]
                value = " ".join(parts[2:])

                self._execute_with_retry(self.db.set, key, value)
                self.cache.delete(key)
                self.queue.publish("KEY_UPDATED", {"key": key, "value": value})
                return "201 CREATED"

            elif action == "DELETE":
                if len(parts) != 2: return "400 BAD REQUEST"
                key = parts[1]

                if self._execute_with_retry(self.db.delete, key):
                    self.cache.delete(key)
                    self.queue.publish("KEY_DELETED", {"key": key})
                    return "200 OK: Deleted"
                return f"404 NOT FOUND: '{key}'"

            elif action == "CHAOS":
                if len(parts) == 2 and parts[1].upper() in ["ON", "OFF"]:
                    mode = parts[1].upper() == "ON"
                    self.db.chaos_mode = mode
                    self.cache.chaos_mode = mode
                    return f"200 OK: Chaos mode is now {'🔥 ON' if mode else '🛑 OFF'}"
                return "400 BAD REQUEST: Use CHAOS ON or CHAOS OFF"

            elif action == "STATS":
                return f"200 OK: Circuit Breaker: {self.db_breaker.state} | Queue: {self.queue.size()}"

            else:
                return f"400 BAD REQUEST: Unknown command '{action}'"

        except CircuitBreakerOpenError as e:
            return f"503 SERVICE UNAVAILABLE: {str(e)}"
        except SimulatedNetworkError:
            return "500 INTERNAL SERVER ERROR: Database connection failed."


# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("Distributed Platform — Phase 5 (Chaos, Retries & Circuit Breakers)")
    print("=" * 70)

    db = Database()
    cache = Cache()
    queue = MessageQueue()
    worker = BackgroundWorker(queue)
    worker.start()

    api = APIGateway(db, cache, queue)

    print("\nAPI Gateway ready. Commands: GET, SET, DELETE, CHAOS ON/OFF, STATS, EXIT.")
    print("─" * 70)

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