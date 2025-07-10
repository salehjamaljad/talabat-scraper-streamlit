import requests
import time
import pandas as pd
import numpy as np
import json
import gspread
import streamlit as st
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from config import branches_uuids, khodar_skus
import pytz
import random

# ————— Static request parts —————
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ar-KW",
    "appbrand": "1",
    "priority": "u=1, i",
    "referer": "https://www.talabat.com/ar/egypt/grocery/650264/talabat-mart?aid=7084",
    "sec-ch-ua": '"Brave";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "sourceapp": "web",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "x-device-source": "0",
}
COOKIES = {
    "tlb_country": "egypt",
    "dhhPerseusGuestId": "1747159326102.939446224868874900.3srp93oyync",
    "tlb_lng": "ar",
    "next-i18next": "ar",
}

def add_summary_row(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if 'price' in numeric_cols:
        numeric_cols.remove('price')
    summary = {col: df[col].sum() for col in numeric_cols}
    summary_row = {
        'sku': 'TOTAL',
        'title': 'TOTAL',
        'category': '',
        'price': None,
        'last_updated': None,
        **summary
    }
    return pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)

def fetch_and_process(branch):
    name = branch["name"]
    uuid = branch["uuid"]
    url = f"https://www.talabat.com/nextApi/groceries/stores/{uuid}/products"

    egypt_tz = pytz.timezone("Africa/Cairo")
    timestamp = datetime.now(egypt_tz).strftime("%Y-%m-%d %H:%M:%S")

    all_items = []
    offset, limit = 0, 200
    print(f"[{name}] request")

    while True:
        params = {
            "countryId":   "9",
            "query":       "khodar.com",
            "limit":       str(limit),
            "offset":      str(offset),
            "isDarkstore": "true",
            "isMigrated":  "false",
        }
        resp = requests.get(url, headers=HEADERS, params=params, cookies=COOKIES)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            break
        all_items.extend(items)
        print(f"fetched {len(items)} items offset={offset}")
        offset += limit
        time.sleep(0.5)

    print(f"[{name}] Crawl complete — total raw items: {len(all_items)}")

    # If no items fetched, return full SKU list with 0 stock and None price
    if not all_items:
        return pd.DataFrame([
            {
                "sku": sku,
                "title": khodar_skus[sku]["title"],
                "category": khodar_skus[sku]["category"],
                f"{name}_stock": 0,
                f"{name}_price": None,
                f"{name}_last_updated": timestamp
            }
            for sku in khodar_skus
        ]).sort_values("sku").reset_index(drop=True)

    # Normal flow for non-empty items
    df = pd.DataFrame([
        {
            "sku": prod.get("sku"),
            "title": None,
            "category": None,
            f"{name}_stock": prod.get("stockAmount", 0),
            f"{name}_price": prod.get("price", None),
            f"{name}_last_updated": timestamp
        }
        for prod in all_items if prod.get("sku")
    ])

    df = df[df["sku"].isin(khodar_skus)].copy()
    df["title"] = df["sku"].map(lambda s: khodar_skus[s]["title"])
    df["category"] = df["sku"].map(lambda s: khodar_skus[s]["category"])

    existing = set(df["sku"])
    missing = set(khodar_skus) - existing
    if missing:
        missing_rows = [
            {
                "sku": sku,
                "title": khodar_skus[sku]["title"],
                "category": khodar_skus[sku]["category"],
                f"{name}_stock": 0,
                f"{name}_price": None,
                f"{name}_last_updated": timestamp
            }
            for sku in missing
        ]
        df = pd.concat([df, pd.DataFrame(missing_rows)], ignore_index=True)

    return df.sort_values("sku").reset_index(drop=True)
    
    
def merge_and_consolidate(dfs):
    tlbt = dfs[0]
    for df_branch in dfs[1:]:
        tlbt = tlbt.merge(df_branch, on=["sku", "title", "category"], how="outer")

    # price and date consolidation
    price_cols = [c for c in tlbt if c.endswith("_price")]
    tlbt["price"] = tlbt[price_cols].bfill(axis=1).iloc[:, 0]
    date_cols = [c for c in tlbt if c.endswith("_last_updated")]
    tlbt["last_updated"] = pd.to_datetime(tlbt[date_cols].bfill(axis=1).iloc[:, 0])

    # stock and total stock
    stock_cols = [c for c in tlbt if c.endswith("_stock")]
    for col in stock_cols:
        tlbt[col] = tlbt[col].fillna(0).astype(int)
    tlbt["total stock"] = tlbt[stock_cols].sum(axis=1)

    final_cols = ["sku", "title", "price"] + stock_cols + ["total stock", "last_updated", "category"]
    return tlbt[final_cols]

def run_all_and_push():
    # — 1) Fetch & process each branch
    dfs = []
    for branch in branches_uuids:
        dfs.append(fetch_and_process(branch))
        wait = random.uniform(2, 5)
        print(f"Waiting {wait:.1f}s before next branch…")
        time.sleep(wait)

    # — 2) Merge into three DataFrames
    talabat_all    = merge_and_consolidate(dfs)
    talabat_first3 = merge_and_consolidate(dfs[:3])
    talabat_rest   = merge_and_consolidate(dfs[3:])

    # — 3) Add summaries for the “Backup” sheets
    talabat_all_summary    = add_summary_row(talabat_all)
    talabat_first3_summary = add_summary_row(talabat_first3)
    talabat_rest_summary   = add_summary_row(talabat_rest)

    # — 4) Authenticate with Google Sheets
    SERVICE_ACCOUNT_DICT = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_DICT, scope)
    client = gspread.authorize(creds)

    # — 5) Spreadsheet keys
    main_key   = "1bHsZvDJQ1U-V3yPalU2gKNRqLOyjtFP509NpHSVj6_k"
    key_first3 = "1d3oDBdu8SqnBlaFDrBDL2lEwe5F9f4RL7RTzK_SRW-4"
    key_rest   = "1cbhFF2daE7iUe0vlebIwohQN3UoxsZWY4I_blqr_8rk"

    # — 6) Write “Backup” tabs (clear + write)
    def push_backup(key, df, title="Backup"):
        sheet  = client.open_by_key(key)
        titles = [ws.title for ws in sheet.worksheets()]
        ws     = sheet.worksheet(title) if title in titles else sheet.add_worksheet(title, rows="1000", cols="60")
        ws.clear()
        set_with_dataframe(ws, df)

    push_backup(main_key,   talabat_all_summary)
    push_backup(key_first3, talabat_first3_summary)
    push_backup(key_rest,   talabat_rest_summary)

    # — 7) Append raw data to “DB” tab without clearing
    sheet_main = client.open_by_key(main_key)
    titles     = [ws.title for ws in sheet_main.worksheets()]
    ws_db      = sheet_main.worksheet("DB") if "DB" in titles else sheet_main.add_worksheet("DB", rows="1000", cols="60")

    # Convert last_updated to string, then sanitize every cell
    db_df = talabat_all.copy()
    db_df["last_updated"] = db_df["last_updated"].dt.strftime("%Y-%m-%d %H:%M:%S")

    def sanitize(val):
        # None or NaT or NaN → None
        if pd.isna(val):
            return None
        # NumPy scalar → native Python
        if isinstance(val, np.generic):
            return val.item()
        # Datetime → ISO string
        if isinstance(val, (pd.Timestamp, datetime)):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        # Otherwise leave as-is (int, float, str)
        return val

    rows = []
    for row in db_df.itertuples(index=False, name=None):
        rows.append([sanitize(cell) for cell in row])

    # Write header if sheet empty
    if not ws_db.get_all_values():
        ws_db.append_row(list(db_df.columns), value_input_option='RAW')

    # Batch-append all data rows
    ws_db.append_rows(rows, value_input_option='RAW')
    print("DB tab appended with new data.")

    return talabat_all_summary, talabat_first3_summary, talabat_rest_summary

if __name__ == "__main__":
    all_df, first3_df, rest_df = run_all_and_push()
