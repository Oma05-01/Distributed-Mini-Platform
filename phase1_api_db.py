"""
Phase 1 — Basic API + Database
==============================
The foundation of our distributed platform.
We have an API Gateway (the CLI loop) that communicates directly
with a Database service.

Commands:
    GET <key>
    SET <key> <value>
    DELETE <key>
    EXIT

Run:
    python phase1_api_db.py
"""

import json
import os
from typing import Optional

DB_FILE = "store_db.json"


# ---------------------------------------------------------------------------
# Service 1: Database Layer
# ---------------------------------------------------------------------------
class Database:
    """
    Simulates the persistent DB you built in Project 2.
    Reads/writes to a JSON file to maintain state across restarts.
    """

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
        print(f"    [DB] READ -> {key}")
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        print(f"    [DB] WRITE -> {key}: {value}")
        self._data[key] = value
        self._flush()

    def delete(self, key: str) -> bool:
        print(f"    [DB] DELETE -> {key}")
        if key in self._data:
            del self._data[key]
            self._flush()
            return True
        return False


# ---------------------------------------------------------------------------
# Service 2: API Gateway
# ---------------------------------------------------------------------------
class APIGateway:
    """
    The entry point for all client requests.
    Right now, it just routes traffic directly to the Database.
    In Phase 2, this gateway will talk to the Cache first.
    """

    def __init__(self, db: Database) -> None:
        self.db = db
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
            value = self.db.get(key)
            if value is None:
                return f"404 NOT FOUND: '{key}'"
            return f"200 OK: {value}"

        elif action == "SET":
            if len(parts) < 3:
                return "400 BAD REQUEST: SET requires a key and a value"
            key = parts[1]
            value = " ".join(parts[2:])
            self.db.set(key, value)
            return "201 CREATED"

        elif action == "DELETE":
            if len(parts) != 2:
                return "400 BAD REQUEST: DELETE requires exactly 1 key"
            key = parts[1]
            success = self.db.delete(key)
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
    print("Distributed Platform — Phase 1 (API + DB)")
    print("=" * 55)

    # Initialize our "services"
    db = Database()
    api = APIGateway(db)

    print("\nAPI Gateway is ready. Type commands (GET, SET, DELETE) or EXIT.")
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

        # Send the raw string to the API Gateway
        response = api.handle_request(line)
        print(f"server ❮ {response}")


if __name__ == "__main__":
    main()