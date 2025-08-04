import os
import json
import requests
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# ----------- CONFIG ------------
PERPS_LIST_PATH = "src/perps.json"
DATA_ROOT = "data/perps"
COMBINED_FOLDER = os.path.join(DATA_ROOT, "combined")
DAILY_COMBINED_FILE = os.path.join(COMBINED_FOLDER, "daily_combined.json")
DERIVATIVES_API_URL = "https://pro-api.coingecko.com/api/v3/derivatives"

# --------- HELPERS -------------
def get_unix_now_utc():
    # Returns current UNIX timestamp (exact time in UTC)
    return int(datetime.now(timezone.utc).timestamp())

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def load_json(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def save_json(path, data):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def get_api_key():
    load_dotenv()
    return os.environ.get("COINGECKO_API_KEY")

def get_perps_exchanges():
    return load_json(PERPS_LIST_PATH)

# --------- LISTINGS HELPERS -----------
LISTINGS_DIR = "listings"
BASE_LIST_FILE = os.path.join(LISTINGS_DIR, "listedtill_5thAug.json")
LISTINGS_LOG_FILE = os.path.join(LISTINGS_DIR, "perps_listings.json")

def ensure_listings_dir():
    ensure_dir(LISTINGS_DIR)

def get_current_listings(all_data):
    listings = {}
    for item in all_data:
        market = item.get("market")
        symbol = item.get("symbol")
        if market and symbol:
            listings.setdefault(market, set()).add(symbol)
    return listings

def load_json_set(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json_pretty(path, data):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)

def save_initial_listed_file(current_listing_map):
    if not os.path.exists(BASE_LIST_FILE):
        print(f"Creating base listings snapshot ({BASE_LIST_FILE}) ...")
        save_json_pretty(BASE_LIST_FILE, {k: list(v) for k, v in current_listing_map.items()})

def load_baseline_listing():
    return load_json_set(BASE_LIST_FILE)

def load_listings_log():
    if os.path.exists(LISTINGS_LOG_FILE):
        with open(LISTINGS_LOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return []

def save_listings_log(log):
    ensure_dir(os.path.dirname(LISTINGS_LOG_FILE))
    with open(LISTINGS_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2)

def update_perps_listings_log(current_listing_map, unix_date):
    baseline = load_baseline_listing()
    log = load_listings_log()
    log.append({
        "date": unix_date,
        "symbol": "NA",
        "name": "NA",
        "action": "last updated"
    })
    baseline_set = set((m, s) for m, syms in baseline.items() for s in syms)
    current_set = set((m, s) for m, syms in current_listing_map.items() for s in syms)
    new_listings = current_set - baseline_set
    for (market, symbol) in new_listings:
        log.append({
            "date": unix_date,
            "symbol": symbol,
            "name": market,
            "action": "listed"
        })
    delistings = baseline_set - current_set
    for (market, symbol) in delistings:
        log.append({
            "date": unix_date,
            "symbol": symbol,
            "name": market,
            "action": "delisted"
        })
    save_listings_log(log)

# --------- MAIN LOGIC -----------

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
            # Defensive: sometimes CoinGecko may return {} or null on errors
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

def save_exchange_daily_data(exchange, tickers, unix_date):
    folder = os.path.join(DATA_ROOT, exchange)
    ensure_dir(folder)
    fname = f"{exchange.lower()}_{unix_date}.json"
    path = os.path.join(folder, fname)

    if os.path.exists(path):
        print(f"File {fname} already exists. Skipping daily save for {exchange}.")
        return fname, False

    data_to_save = []
    for t in tickers:
        symbol = t.get("symbol", "")
        open_interest = t.get("open_interest")
        volume_24h = t.get("volume_24h")
        # Defensive null handling: use 0 if missing or None
        try:
            open_interest = float(open_interest) if open_interest not in [None, ""] else 0
        except Exception:
            open_interest = 0
        try:
            volume_24h = float(volume_24h) if volume_24h not in [None, ""] else 0
        except Exception:
            volume_24h = 0
        data_to_save.append({
            "symbol": symbol,
            "open_interest": open_interest,
            "volume_24h": volume_24h
        })
    save_json(path, data_to_save)
    print(f"Saved: {path}")

    # Update combined index
    combined_index_path = os.path.join(folder, f"combined_{exchange.lower()}.json")
    combined = load_json(combined_index_path) or []
    if fname not in combined:
        combined.append(fname)
        save_json(combined_index_path, combined)

    return fname, True

def update_daily_combined(exchange, sum_volume, unix_date):
    ensure_dir(COMBINED_FOLDER)
    combined_data = load_json(DAILY_COMBINED_FILE) or []

    updated = False
    for entry in combined_data:
        if entry["date"] == unix_date and entry["market"].lower() == exchange.lower():
            entry["sum_volume_24h"] = sum_volume
            updated = True
            break
    if not updated:
        combined_data.append({
            "date": unix_date,
            "market": exchange,
            "sum_volume_24h": sum_volume
        })
    save_json(DAILY_COMBINED_FILE, combined_data)

def main():
    print("Starting Perps Data Fetcher")
    ensure_listings_dir()
    api_key = get_api_key()
    if not api_key:
        print("ERROR: COINGECKO_API_KEY not found in .env")
        return

    perps_list = get_perps_exchanges()
    if not perps_list or not isinstance(perps_list, list):
        print(f"ERROR: Could not read valid perps.json from {PERPS_LIST_PATH}")
        return

    all_data = fetch_derivatives_data(api_key)
    if not all_data:
        print("No data fetched from CoinGecko. Exiting.")
        return

    unix_date = get_unix_now_utc()
    # --- LISTINGS LOGIC ---
    current_listing_map = get_current_listings(all_data)
    save_initial_listed_file(current_listing_map)
    update_perps_listings_log(current_listing_map, unix_date)
    # --- END LISTINGS LOGIC ---

    for ex in perps_list:
        ex_name = ex["name"]
        # Collect all tickers for this exchange
        ex_tickers = [item for item in all_data if item.get("market", "").lower() == ex_name.lower()]
        if not ex_tickers:
            print(f"No tickers found for {ex_name}")
            continue

        fname, saved = save_exchange_daily_data(ex_name, ex_tickers, unix_date)

        # Sum up all tickers' 24h volume for combined, null-safe
        sum_volume = 0
        for t in ex_tickers:
            v = t.get("volume_24h")
            try:
                sum_volume += float(v) if v not in [None, ""] else 0
            except Exception:
                pass
        update_daily_combined(ex_name, sum_volume, unix_date)

    print("Done.")

if __name__ == "__main__":
    main()
