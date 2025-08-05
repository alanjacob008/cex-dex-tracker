import os
import json
import requests
import time
from datetime import datetime, timezone

PERPS_LIST_PATH = "src/perps.json"
DATA_ROOT = "data/perps"
COMBINED_FOLDER = os.path.join(DATA_ROOT, "combined")
DAILY_COMBINED_FILE = os.path.join(COMBINED_FOLDER, "daily_combined.json")
DERIVATIVES_API_URL = "https://pro-api.coingecko.com/api/v3/derivatives"
MAX_JSON_MB = 3

def get_unix_now_utc():
    return int(datetime.now(timezone.utc).timestamp())

def sanitize(name):
    return name.replace(" ", "_").replace("(", "_").replace(")", "_").replace("-", "_")

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
    from dotenv import load_dotenv
    load_dotenv()
    return os.environ.get("COINGECKO_API_KEY")

def get_perps_exchanges():
    return load_json(PERPS_LIST_PATH)

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

def get_current_json_path(folder, base_name):
    """
    Find the last (biggest suffix) json for this exchange, or start with 1 if none.
    """
    existing = []
    for fname in os.listdir(folder):
        if fname.startswith(base_name) and fname.endswith(".json"):
            parts = fname[:-5].rsplit("_", 1)
            if len(parts) == 2 and parts[1].isdigit():
                existing.append(int(parts[1]))
    suffix = max(existing) if existing else 1
    return os.path.join(folder, f"{base_name}_{suffix:05}.json")

def rotate_json_if_needed(folder, base_name):
    """
    Returns path to append into: if current file is >3MB, make a new one.
    """
    path = get_current_json_path(folder, base_name)
    if os.path.exists(path) and os.path.getsize(path) >= MAX_JSON_MB * 1024 * 1024:
        # rotate: increment suffix
        suffix = int(path[-10:-5]) + 1
        path = os.path.join(folder, f"{base_name}_{suffix:05}.json")
    return path

def save_exchange_data(exchange, tickers, unix_date):
    safe_ex_name = sanitize(exchange.lower())
    folder = os.path.join(DATA_ROOT, safe_ex_name)
    ensure_dir(folder)
    base_name = safe_ex_name

    # Find file to append into
    path = rotate_json_if_needed(folder, base_name)

    # Load or create list
    existing = load_json(path)
    if not isinstance(existing, list):
        existing = []

    for t in tickers:
        symbol = t.get("symbol", "")
        open_interest = t.get("open_interest")
        volume_24h = t.get("volume_24h")
        try:
            open_interest = float(open_interest) if open_interest not in [None, ""] else 0
        except Exception:
            open_interest = 0
        try:
            volume_24h = float(volume_24h) if volume_24h not in [None, ""] else 0
        except Exception:
            volume_24h = 0
        row = {
            "symbol": symbol,
            "open_interest": open_interest,
            "volume_24h": volume_24h,
            "date": unix_date
        }
        existing.append(row)

    save_json(path, existing)
    print(f"Appended {len(tickers)} rows to: {path}")

    # Update combined_index
    combined_index_path = os.path.join(folder, f"combined_{base_name}.json")
    index = load_json(combined_index_path) or []
    if os.path.basename(path) not in index:
        index.append(os.path.basename(path))
        save_json(combined_index_path, index)

def update_daily_combined(exchange, sum_volume, unix_date):
    ensure_dir(COMBINED_FOLDER)
    combined_data = load_json(DAILY_COMBINED_FILE) or []
    combined_data.append({
        "date": unix_date,
        "market": sanitize(exchange),
        "sum_volume_24h": sum_volume
    })
    save_json(DAILY_COMBINED_FILE, combined_data)

def main():
    print("Starting Perps Data Fetcher")
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
    for ex in perps_list:
        ex_name = ex["name"]
        safe_ex_name = sanitize(ex_name)
        ex_tickers = [item for item in all_data if item.get("market", "").lower() == ex_name.lower()]
        if not ex_tickers:
            print(f"No tickers found for {ex_name}")
            continue

        save_exchange_data(ex_name, ex_tickers, unix_date)

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
