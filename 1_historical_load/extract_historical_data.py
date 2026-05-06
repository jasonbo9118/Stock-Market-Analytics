import requests
from dotenv import load_dotenv
import os
import datetime as dt
import time
import csv

load_dotenv()

API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE_URL = "https://api.massive.com"
SLEEP_SECONDS = 12


def get_stock_prices(req_date):
    url = f"{POLYGON_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{req_date}"

    params = {
        "adjusted": "true",
        "include_otc": "false",
        "apiKey": API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])

    except Exception as e:
        print(f"Error fetching {req_date}: {e}")
        return []


def extract_historical_data():
    start_date = dt.date(year=2025, month=10, day=17)
    end_date = dt.date(year=2025, month=10, day=24)

    out_file = (
        f"polygon_eod_grouped_"
        f"{start_date.strftime('%Y%m%d')}_"
        f"{end_date.strftime('%Y%m%d')}.csv"
    )

    ingest_ts = (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "date",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "_src_file",
            "_ingest_ts"
        ])

        current_date = start_date

        while current_date <= end_date:
            print(f"Fetching {current_date} ...")

            results = get_stock_prices(current_date)

            if results:
                print(f"{current_date}: {len(results)} rows")

                for row in results:
                    writer.writerow([
                        current_date,
                        row.get("T", ""),
                        row.get("o", ""),
                        row.get("h", ""),
                        row.get("l", ""),
                        row.get("c", ""),
                        row.get("v", ""),
                        out_file,
                        ingest_ts
                    ])

                f.flush()

            else:
                print(f"No results for {current_date}, it may be weekend or holiday.")

            current_date += dt.timedelta(days=1)
            time.sleep(SLEEP_SECONDS)

    print(f"\nDone! Saved: {out_file}")


if __name__ == "__main__":
    extract_historical_data()