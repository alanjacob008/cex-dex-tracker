import os
import json
from datetime import datetime, timezone

# ----------- CONFIG -----------
BASE_LIST_FILE = "listings/listedtill_5thAug.json"
LOG_FILE = "listings/perps_listings.json"
CURRENT_LIST_FILE = "listings/dummy_listings.json"  # Should be a snapshot from latest API
DUMMY_ENTRY = {
    "date": None,
    "symbol": "NA",
    "name": "NA",
    "action": "last updated"
}

def get_unix_now_utc():
    return int(datetime.now(timezone.utc).timestamp())

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_log(log):
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)

def main():
    baseline = load_json(BASE_LIST_FILE)
    current = load_json(CURRENT_LIST_FILE)
    log = load_log()
    now = get_unix_now_utc()

    # ---- update or add dummy ----
    found = False
    for entry in log:
        if entry.get("action") == "last updated":
            entry["date"] = now
            found = True
            break
    if not found:
        dummy = DUMMY_ENTRY.copy()
        dummy["date"] = now
        log.insert(0, dummy)

    # ---- find new listings and delistings ----
    # Flatten to sets for easy comparison
    baseline_set = set((ex, sym) for ex, syms in baseline.items() for sym in syms)
    current_set = set((ex, sym) for ex, syms in current.items() for sym in syms)

    # New listings
    for ex, sym in sorted(current_set - baseline_set):
        log.append({
            "date": now,
            "symbol": sym,
            "name": ex,
            "action": "listed"
        })

    # Delistings
    for ex, sym in sorted(baseline_set - current_set):
        log.append({
            "date": now,
            "symbol": sym,
            "name": ex,
            "action": "delisted"
        })

    save_log(log)
    print("Listings check completed, log updated.")

if __name__ == "__main__":
    main()
