import random
import time

import pandas as pd
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

MSC_TRACKING_URL = 'https://www.msc.com/api/feature/tools/TrackingInfo'

HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'content-type': 'application/json',
    'origin': 'https://www.msc.com',
    'referer': 'https://www.msc.com/en/track-a-shipment',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
}

# Map event descriptions to (date column, location column)
EVENT_COLUMNS = {
    'Estimated Time of Arrival': ('ETA Date', 'ETA Location'),
    'Import Loaded on Rail': ('Rail Load Date', 'Rail Load Location'),
    'Import Discharged from Vessel': ('Discharged Date', 'Discharged Location')
}


def fetch_tracking_data(bol):
    try:
        response = requests.post(
            MSC_TRACKING_URL,
            headers=HEADERS,
            json={'trackingNumber': bol, 'trackingMode': '0'},
            timeout=15,
            impersonate='chrome110'
        )
        time.sleep(random.randint(1, 3))

        json_data = response.json()
        bill_info = json_data['Data']['BillOfLadings'][0]
        shipped_from = bill_info['GeneralTrackingInfo'].get('ShippedFrom')
        discharge_port = bill_info['GeneralTrackingInfo'].get('PortOfDischarge')

        record = {
            "BillOfLading": bol,
            "ShippedFrom": shipped_from,
            "PortOfDischarge": discharge_port
        }

        for container in bill_info.get('ContainersInfo', []):
            for event in container.get('Events', []):
                desc = event.get('Description')
                if desc in EVENT_COLUMNS:
                    date_col, loc_col = EVENT_COLUMNS[desc]
                    # Prefer latest event if already filled — keep most recent per container
                    if date_col not in record:
                        record[date_col] = event.get('Date')
                        record[loc_col] = event.get('Location')

        return record
    except Exception as e:
        print(f"[ERROR] Failed for BOL {bol}: {e}")
        return {}


def track_msc_shipments(df: pd.DataFrame, max_workers: int = 4) -> pd.DataFrame:
    bol_list = df['B/L No.'].dropna().unique().tolist()

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_tracking_data, bol): bol for bol in bol_list}
        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)

    return pd.DataFrame(results)
