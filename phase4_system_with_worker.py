"""
Phase 4 — Background Worker (Asynchronous Processing)
=====================================================
We introduce a Background Worker that runs in its own thread.
It constantly monitors the Message Queue and processes events
(simulating tasks like sending emails or updating analytics)
without blocking the main API Gateway.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    STATS
    EXIT

Run:
    python phase4_system_with_worker.py
"""

import json
import os
import time
import threading
from typing import Optional

DB_FILE = "store_db.json"


# ---------------------------------------------------------------------------
# Service 1: Database Layer
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, path: str = DB_FILE) -> None:
        self.path = path
        self._data: dict[str, str] = self._load()
        print("  [System] Database service initialized.")

    def _load(self) -> dict[str, str]:
        if not os.path.exists(self.path): return {}
        with open(self.path, "r", encoding="utf-8") as f: return json.load(f)

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f: json.dump(self._data, f, indent=2)

    def get(self, key: str) -> Optional[str]:
        time.sleep(0.5)
        print(f"    [DB] READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        time.sleep(0.5)
        print(f"    [DB] WRITE -> '{key}': '{value}'")
        self._data[key] = value
        self._flush()

    def delete(self, key: str) -> bool:
        time.sleep(0.5)
        print(f"    [DB] DELETE -> '{key}'")
        if key in self._data:
            del self._data[key]
            self._flush()
            return True
        return False


# ---------------------------------------------------------------------------
# Service 2: Cache Layer
# ---------------------------------------------------------------------------
class Cache:
    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        print("  [System] Cache service initialized.")

    def get(self, key: str) -> Optional[str]:
        print(f"    [Cache] READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        print(f"    [Cache] WRITE -> '{key}': '{value}'")
        self._data[key] = value

    def delete(self, key: str) -> None:
        print(f"    [Cache] DELETE -> '{key}'")
        self._data.pop(key, None)


# ---------------------------------------------------------------------------
# Service 3: Message Queue Layer
# ---------------------------------------------------------------------------
class MessageQueue:
    def __init__(self) -> None:
        self._events: list[dict] = []
        print("  [System] Message Queue service initialized.")

    def publish(self, event_type: str, payload: dict) -> None:
        event = {"type": event_type, "payload": payload, "timestamp": time.time()}
        self._events.append(event)
        print(f"    [Queue] PUBLISHED -> {event_type} | Backlog: {len(self._events)}")

    def consume(self) -> Optional[dict]:
        """Pops the oldest event from the queue."""
        if self._events:
            return self._events.pop(0)
        return None

    def size(self) -> int:
        return len(self._events)


# ---------------------------------------------------------------------------
# Service 4: Background Worker (NEW)
# ---------------------------------------------------------------------------
class BackgroundWorker:
    """Runs in a separate thread to process queue events asynchronously."""

    def __init__(self, queue: MessageQueue) -> None:
        self.queue = queue
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        print("  [System] Background Worker initialized.")

    def start(self) -> None:
        self.thread.start()
        print("  [System] Background Worker started in a separate thread.")

    def stop(self) -> None:
        self.running = False

    def _run_loop(self) -> None:
        while self.running:
            event = self.queue.consume()
            if event:
                self._process_event(event)
            else:
                # Sleep briefly to prevent maxing out the CPU when queue is empty
                time.sleep(1.0)

    def _process_event(self, event: dict) -> None:
        event_type = event["type"]
        payload = event["payload"]

        # Simulate time-consuming work (e.g., calling external 3rd party APIs)
        print(f"\n    [Worker] ⚙️ Processing event: {event_type} for key '{payload.get('key')}'...")
        time.sleep(2.0)

        if event_type == "KEY_UPDATED":
            print(f"    [Worker] ✅ DONE: Sent welcome email / updated analytics for '{payload.get('key')}'.")
        elif event_type == "KEY_DELETED":
            print(f"    [Worker] ✅ DONE: Cleaned up downstream records for '{payload.get('key')}'.")

        # Reprint the prompt so the user isn't left hanging (CLI quirk)
        print("\nclient ❯ ", end="", flush=True)


# ---------------------------------------------------------------------------
# Service 5: API Gateway
# ---------------------------------------------------------------------------
class APIGateway:
    def __init__(self, db: Database, cache: Cache, queue: MessageQueue) -> None:
        self.db = db
        self.cache = cache
        self.queue = queue
        print("  [System] API Gateway initialized.")

    def handle_request(self, command: str) -> str:
        parts = command.strip().split()
        if not parts: return ""
        action = parts[0].upper()

        if action == "GET":
            if len(parts) != 2: return "400 BAD REQUEST"
            key = parts[1]

            value = self.cache.get(key)
            if value is not None:
                print("    [Gateway] CACHE HIT ⚡")
                return f"200 OK: {value}"

            print("    [Gateway] CACHE MISS 🐌")
            value = self.db.get(key)
            if value is None:
                return f"404 NOT FOUND: '{key}'"

            self.cache.set(key, value)
            return f"200 OK: {value}"

        elif action == "SET":
            if len(parts) < 3: return "400 BAD REQUEST"
            key = parts[1]
            value = " ".join(parts[2:])

            self.db.set(key, value)
            self.cache.delete(key)
            self.queue.publish("KEY_UPDATED", {"key": key, "value": value})
            return "201 CREATED"

        elif action == "DELETE":
            if len(parts) != 2: return "400 BAD REQUEST"
            key = parts[1]

            if self.db.delete(key):
                self.cache.delete(key)
                self.queue.publish("KEY_DELETED", {"key": key})
                return "200 OK: Deleted"
            return f"404 NOT FOUND: '{key}'"

        elif action == "STATS":
            return f"200 OK: System Stats -> DB: {len(self.db._data)}, Cache: {len(self.cache._data)}, Queue Backlog: {self.queue.size()}"

        else:
            return f"400 BAD REQUEST: Unknown command '{action}'"


# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def main():
    print("=" * 65)
    print("Distributed Platform — Phase 4 (Async Worker Integration)")
    print("=" * 65)

    db = Database()
    cache = Cache()
    queue = MessageQueue()

    # Initialize and start the background worker thread
    worker = BackgroundWorker(queue)
    worker.start()

    api = APIGateway(db, cache, queue)

    print("\nAPI Gateway is ready. Commands: GET, SET, DELETE, STATS, EXIT.")
    print("Notice how the Worker processes events in the background while you type!")
    print("─" * 65)

    while True:
        try:
            line = input("\nclient ❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nShutting down services...")
            worker.stop()
            break

        if not line: continue
        if line.upper() == "EXIT":
            print("Shutting down services...")
            worker.stop()
            break

        response = api.handle_request(line)
        print(f"server ❮ {response}")


if __name__ == "__main__":
    main()