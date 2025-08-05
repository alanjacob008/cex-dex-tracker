import os
import json
import requests
import time
from datetime import datetime, timezone

BASE_LIST_FILE = "listings/listedtill_5thAug.json"
LOG_FILE = "listings/perps_listings.json"
DERIVATIVES_API_URL = "https://pro-api.coingecko.com/api/v3/derivatives"
PERPS_JSON = "src/perps.json"

def load_perps_exchanges():
    if os.path.exists(PERPS_JSON):
        with open(PERPS_JSON, "r", encoding="utf-8") as f:
            perps = json.load(f)
            return set(ex['name'] for ex in perps if 'name' in ex)
    return set()

def get_api_key():
    from dotenv import load_dotenv
    load_dotenv()
    return os.environ.get("COINGECKO_API_KEY")

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

def fetch_derivatives_data(api_key, retries=3, wait_sec=30):
    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": api_key
    }
    attempt = 0
    while attempt < retries:
        try:
            resp = requests.get(DERIVATIVES_API_URL, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                return data
            print(f"Warning: API returned no data or invalid format, attempt {attempt + 1}")
        except Exception as e:
            print(f"Error fetching CoinGecko API (attempt {attempt + 1}): {e}")
        attempt += 1
        if attempt < retries:
            print(f"Retrying in {wait_sec} seconds...")
            time.sleep(wait_sec)
    print("ERROR: CoinGecko API failed after 3 attempts, exiting.")
    return None

def build_last_state(log):
    """
    Return a dict of (ex, sym) -> last action seen ("listed"/"delisted"), ignoring 'last updated'
    Only the last action is remembered per pair.
    """
    state = {}
    for entry in log:
        if entry.get("action") in ("listed", "delisted"):
            key = (entry.get("name"), entry.get("symbol"))
            state[key] = entry.get("action")
    return state

def main():
    tracked_exchanges = load_perps_exchanges()
    if not tracked_exchanges:
        print(f"ERROR: Could not load tracked exchanges from {PERPS_JSON}")
        return

    api_key = get_api_key()
    if not api_key:
        print("ERROR: COINGECKO_API_KEY not found in .env")
        return

    # --- Load Baseline ---
    baseline = load_json(BASE_LIST_FILE) # {exchange: [symbols, ...]}
    if not baseline:
        print(f"ERROR: Could not load baseline from {BASE_LIST_FILE}")
        return

    # --- Fetch Live Data ---
    live_data = fetch_derivatives_data(api_key)
    if not live_data:
        print("ERROR: No live data from CoinGecko.")
        return

    now = get_unix_now_utc()

    # --- Map to {exchange: set(symbols)} ---
    base_map = {ex: set(syms) for ex, syms in baseline.items() if ex in tracked_exchanges}
    live_map = {}
    for item in live_data:
        ex = item.get("market")
        sym = item.get("symbol")
        if not ex or not sym or ex not in tracked_exchanges:
            continue
        if ex not in live_map:
            live_map[ex] = set()
        live_map[ex].add(sym)

    # --- Load or Init Log ---
    log = load_json(LOG_FILE) if os.path.exists(LOG_FILE) else []
    # Always update 'last updated' as the first row
    if log and log[0].get("action") == "last updated":
        log[0]["date"] = now
    else:
        log = [{
            "date": now,
            "symbol": "NA",
            "name": "NA",
            "action": "last updated"
        }] + log

    # --- Build last known state of each (exchange, symbol) ---
    last_state = build_last_state(log)

    # --- Listings & Delistings ---
    listed, delisted = [], []
    # Listings: in live, not in base
    for ex, syms in live_map.items():
        base_syms = base_map.get(ex, set())
        for sym in syms:
            if sym not in base_syms:
                # Only log if not already 'listed'
                if last_state.get((ex, sym)) != "listed":
                    listed.append((ex, sym))
    # Delistings: in base, not in live
    for ex, syms in base_map.items():
        live_syms = live_map.get(ex, set())
        for sym in syms:
            if sym not in live_syms:
                # Only log if not already 'delisted'
                if last_state.get((ex, sym)) != "delisted":
                    delisted.append((ex, sym))

    for ex, sym in listed:
        log.append({
            "date": now,
            "symbol": sym,
            "name": ex,
            "action": "listed"
        })
    for ex, sym in delisted:
        log.append({
            "date": now,
            "symbol": sym,
            "name": ex,
            "action": "delisted"
        })

    save_json(LOG_FILE, log)

    print(f"Listings checked at {now}: {len(listed)} listed, {len(delisted)} delisted")
    print(f"perps_listings.json updated")

if __name__ == "__main__":
    main()
