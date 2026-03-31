"""
Phase 2 — Cache Layer (Cache-Aside Pattern)
===========================================
We introduce an in-memory Cache service.
The API Gateway now checks the Cache first. If the data is missing (Cache Miss),
it falls back to the Database, and then updates the Cache for future requests.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    EXIT

Run:
    python phase2_api_cache_db.py
"""

import json
import os
import time
from typing import Optional

DB_FILE = "store_db.json"


# ---------------------------------------------------------------------------
# Service 1: Database Layer
# ---------------------------------------------------------------------------
class Database:
    """Persistent DB. Added artificial delay to simulate disk/network I/O."""

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
        time.sleep(0.5)  # Simulate slow DB read
        print(f"    [DB] READ -> '{key}'")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        time.sleep(0.5)  # Simulate slow DB write
        print(f"    [DB] WRITE -> '{key}': '{value}'")
        self._data[key] = value
        self._flush()

    def delete(self, key: str) -> bool:
        time.sleep(0.5)  # Simulate slow DB delete
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
    """Fast, in-memory volatile storage. No artificial delay."""

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
# Service 3: API Gateway
# ---------------------------------------------------------------------------
class APIGateway:
    """
    Orchestrates the Cache-Aside pattern.
    Reads: Check Cache -> If miss, check DB -> Update Cache.
    Writes: Write to DB -> Invalidate (delete) from Cache.
    """

    def __init__(self, db: Database, cache: Cache) -> None:
        self.db = db
        self.cache = cache
        print("  [System] API Gateway initialized.")

    def handle_request(self, command: str) -> str:
        parts = command.strip().split()
        if not parts:
            return ""

        action = parts[0].upper()

        if action == "GET":
            if len(parts) != 2:
                return "400 BAD REQUEST: GET requires exactly 1 key"
            key = parts[1]

            # 1. Check Cache first
            value = self.cache.get(key)
            if value is not None:
                print("    [Gateway] CACHE HIT ⚡")
                return f"200 OK: {value}"

            # 2. Cache Miss -> Check DB
            print("    [Gateway] CACHE MISS 🐌 (Falling back to DB)")
            value = self.db.get(key)
            if value is None:
                return f"404 NOT FOUND: '{key}'"

            # 3. Update Cache for next time
            print("    [Gateway] Updating cache with DB result...")
            self.cache.set(key, value)
            return f"200 OK: {value}"

        elif action == "SET":
            if len(parts) < 3:
                return "400 BAD REQUEST: SET requires a key and a value"
            key = parts[1]
            value = " ".join(parts[2:])

            # 1. Write to DB (Source of Truth)
            self.db.set(key, value)

            # 2. Invalidate Cache (Stale data removal)
            self.cache.delete(key)
            return "201 CREATED"

        elif action == "DELETE":
            if len(parts) != 2:
                return "400 BAD REQUEST: DELETE requires exactly 1 key"
            key = parts[1]

            # 1. Delete from DB
            success = self.db.delete(key)

            # 2. Invalidate Cache
            self.cache.delete(key)

            if success:
                return "200 OK: Deleted"
            return f"404 NOT FOUND: '{key}'"

        else:
            return f"400 BAD REQUEST: Unknown command '{action}'"


# ---------------------------------------------------------------------------
# CLI REPL
# ---------------------------------------------------------------------------
def main():
    print("=" * 55)
    print("Distributed Platform — Phase 2 (API + Cache + DB)")
    print("=" * 55)

    db = Database()
    cache = Cache()
    api = APIGateway(db, cache)

    print("\nAPI Gateway is ready. Type commands (GET, SET, DELETE) or EXIT.")
    print("Try GETting the same key twice to see the cache hit!")
    print("─" * 55)

    while True:
        try:
            line = input("\nclient ❯ ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nShutting down services...")
            break

        if not line:
            continue
        if line.upper() == "EXIT":
            print("Shutting down services...")
            break

        response = api.handle_request(line)
        print(f"server ❮ {response}")


if __name__ == "__main__":
    main()