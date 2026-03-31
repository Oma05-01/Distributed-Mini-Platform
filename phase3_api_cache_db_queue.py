"""
Phase 3 — Message Queue (Asynchronous Events)
=============================================
We introduce an in-memory Message Queue.
When the API Gateway processes a write (SET or DELETE), it publishes an event
to the queue so that secondary tasks can be handled asynchronously later.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    STATS           <-- New command to check system state
    EXIT

Run:
    python phase3_api_cache_db_queue.py
"""

import json
import os
import time
from typing import Optional

DB_FILE = "store_db.json"


# ---------------------------------------------------------------------------
# Service 1: Database Layer (Same as Phase 2)
# ---------------------------------------------------------------------------
class Database:
    def __init__(self, path: str = DB_FILE) -> None:
        self.path = path
        self._data: dict[str, str] = self._load()
        print("  [System] Database service initialized.")

    def _load(self) -> dict[str, str]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _flush(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

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
# Service 2: Cache Layer (Same as Phase 2)
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
# Service 3: Message Queue Layer (NEW)
# ---------------------------------------------------------------------------
class MessageQueue:
    """Fast, in-memory queue for decoupling background work."""

    def __init__(self) -> None:
        self._events: list[dict] = []
        print("  [System] Message Queue service initialized.")

    def publish(self, event_type: str, payload: dict) -> None:
        event = {"type": event_type, "payload": payload, "timestamp": time.time()}
        self._events.append(event)
        print(f"    [Queue] PUBLISHED -> {event_type} | Pending events: {len(self._events)}")

    def size(self) -> int:
        return len(self._events)


# ---------------------------------------------------------------------------
# Service 4: API Gateway (Updated)
# ---------------------------------------------------------------------------
class APIGateway:
    def __init__(self, db: Database, cache: Cache, queue: MessageQueue) -> None:
        self.db = db
        self.cache = cache
        self.queue = queue
        print("  [System] API Gateway initialized.")

    def handle_request(self, command: str) -> str:
        parts = command.strip().split()
        if not parts:
            return ""

        action = parts[0].upper()

        if action == "GET":
            if len(parts) != 2: return "400 BAD REQUEST"
            key = parts[1]

            value = self.cache.get(key)
            if value is not None:
                print("    [Gateway] CACHE HIT ⚡")
                return f"200 OK: {value}"

            print("    [Gateway] CACHE MISS 🐌 (Falling back to DB)")
            value = self.db.get(key)
            if value is None:
                return f"404 NOT FOUND: '{key}'"

            print("    [Gateway] Updating cache with DB result...")
            self.cache.set(key, value)
            return f"200 OK: {value}"

        elif action == "SET":
            if len(parts) < 3: return "400 BAD REQUEST"
            key = parts[1]
            value = " ".join(parts[2:])

            self.db.set(key, value)
            self.cache.delete(key)

            # --- NEW: Publish Event to Queue ---
            self.queue.publish("KEY_UPDATED", {"key": key, "value": value})

            return "201 CREATED"

        elif action == "DELETE":
            if len(parts) != 2: return "400 BAD REQUEST"
            key = parts[1]

            success = self.db.delete(key)
            self.cache.delete(key)

            if success:
                # --- NEW: Publish Event to Queue ---
                self.queue.publish("KEY_DELETED", {"key": key})
                return "200 OK: Deleted"
            return f"404 NOT FOUND: '{key}'"

        elif action == "STATS":
            return f"200 OK: System Stats -> DB Keys: {len(self.db._data)}, Cache Keys: {len(self.cache._data)}, Queue Backlog: {self.queue.size()}"

        else:
            return f"400 BAD REQUEST: Unknown command '{action}'"


# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Distributed Platform — Phase 3 (API + Cache + DB + Queue)")
    print("=" * 60)

    db = Database()
    cache = Cache()
    queue = MessageQueue()
    api = APIGateway(db, cache, queue)

    print("\nAPI Gateway is ready. Commands: GET, SET, DELETE, STATS, EXIT.")
    print("─" * 60)

    while True:
        try:
            line = input("\nclient ❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line: continue
        if line.upper() == "EXIT": break

        response = api.handle_request(line)
        print(f"server ❮ {response}")


if __name__ == "__main__":
    main()