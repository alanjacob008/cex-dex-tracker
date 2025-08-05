import os
import json
from datetime import datetime, timezone

BASE_LIST_FILE = "listings/listedtill_5thAug.json"
CURRENT_LIST_FILE = "listings/dummy_listings.json"  # for testing
LOG_FILE = "listings/perps_listings.json"

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

def normalize(s):
    return str(s).strip().lower().replace(" ", "").replace("_", "")

def make_pair_set(listing):
    # Returns set of (exchange, symbol) tuples, all normalized
    pairs = set()
    for exch, syms in listing.items():
        for sym in syms:
            pairs.add((normalize(exch), normalize(sym)))
    return pairs

def main():
    baseline = load_json(BASE_LIST_FILE)
    current = load_json(CURRENT_LIST_FILE)
    log = load_json(LOG_FILE) if os.path.exists(LOG_FILE) else []
    now = get_unix_now_utc()

    # Update or add the dummy "last updated" entry (always as first element)
    if log and log[0].get("action") == "last updated":
        log[0]["date"] = now
    else:
        log = [{
            "date": now,
            "symbol": "NA",
            "name": "NA",
            "action": "last updated"
        }] + log

    # Build normalized sets
    baseline_set = make_pair_set(baseline)
    current_set = make_pair_set(current)

    # Find listings and delistings
    listed = sorted(current_set - baseline_set)
    delisted = sorted(baseline_set - current_set)
    count_listed, count_delisted = 0, 0

    for exch, sym in listed:
        log.append({
            "date": now,
            "symbol": sym,
            "name": exch,
            "action": "listed"
        })
        count_listed += 1
    for exch, sym in delisted:
        log.append({
            "date": now,
            "symbol": sym,
            "name": exch,
            "action": "delisted"
        })
        count_delisted += 1

    save_json(LOG_FILE, log)

    print(f"Listings check complete: {count_listed} listed, {count_delisted} delisted.")
    print(f"Log written to {LOG_FILE}")

if __name__ == "__main__":
    main()
