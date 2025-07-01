import requests
import time
import pandas as pd
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

# Fetch and process a single branch

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

    # Filter and enrich
    df = df[df["sku"].isin(khodar_skus)].copy()
    df["title"] = df["sku"].map(lambda s: khodar_skus[s]["title"])
    df["category"] = df["sku"].map(lambda s: khodar_skus[s]["category"])

    # Inject missing SKUs
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

# Merge & consolidate a list of branch DataFrames

def merge_and_consolidate(dfs):
    tlbt = dfs[0]
    for df_branch in dfs[1:]:
        tlbt = tlbt.merge(df_branch, on=["sku", "title", "category"], how="outer")

    # Consolidate price & last_updated
    price_cols = [c for c in tlbt if c.endswith("_price")]
    tlbt["price"] = tlbt[price_cols].bfill(axis=1).iloc[:, 0]

    date_cols = [c for c in tlbt if c.endswith("_last_updated")]
    tlbt["last_updated"] = pd.to_datetime(tlbt[date_cols].bfill(axis=1).iloc[:, 0])

    # Fill stock and compute total
    stock_cols = [c for c in tlbt if c.endswith("_stock")]
    for col in stock_cols:
        tlbt[col] = tlbt[col].fillna(0).astype(int)
    tlbt["total stock"] = tlbt[stock_cols].sum(axis=1)

    final_cols = ["sku", "title", "price"] + stock_cols + ["total stock", "last_updated", "category"]
    return tlbt[final_cols]

# Main runner: fetch once, split, merge, and push to 3 spreadsheets

def run_all_and_push():
    # 1) Fetch for every branch with delay to avoid blocking
    dfs = []
    for branch in branches_uuids:
        df = fetch_and_process(branch)
        dfs.append(df)
        wait = random.uniform(2, 5)
        print(f"Waiting {wait:.1f}s before next branch…")
        time.sleep(wait)

    # 2) Split into first 3 and the rest
    dfs_first3 = dfs[:3]
    dfs_rest = dfs[3:]

    # 3) Merge & consolidate each set
    talabat_all = merge_and_consolidate(dfs)
    talabat_first3 = merge_and_consolidate(dfs_first3)
    talabat_rest = merge_and_consolidate(dfs_rest)

    # 4) Authenticate once for Google Sheets
    SERVICE_ACCOUNT_DICT = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_DICT, scope)
    client = gspread.authorize(creds)

    # 5) Push to main spreadsheet
    main_key = "1bHsZvDJQ1U-V3yPalU2gKNRqLOyjtFP509NpHSVj6_k"
    sheet_main = client.open_by_key(main_key)
    ws_main = sheet_main.worksheet("Backup") if "Backup" in [ws.title for ws in sheet_main.worksheets()] else sheet_main.add_worksheet("Backup", rows="1000", cols="60")
    ws_main.clear()
    set_with_dataframe(ws_main, talabat_all)
    print("Main spreadsheet updated.")

    # 6) Push first 3 to separate spreadsheet
    key_first3 = "1d3oDBdu8SqnBlaFDrBDL2lEwe5F9f4RL7RTzK_SRW-4"
    sheet_f3 = client.open_by_key(key_first3)
    ws_f3 = sheet_f3.worksheet("Backup") if "Backup" in [ws.title for ws in sheet_f3.worksheets()] else sheet_f3.add_worksheet("Backup", rows="1000", cols="60")
    ws_f3.clear()
    set_with_dataframe(ws_f3, talabat_first3)
    print("First 3 branches spreadsheet updated.")

    # 7) Push rest to separate spreadsheet
    key_rest = "1cbhFF2daE7iUe0vlebIwohQN3UoxsZWY4I_blqr_8rk"
    sheet_rt = client.open_by_key(key_rest)
    ws_rt = sheet_rt.worksheet("Backup") if "Backup" in [ws.title for ws in sheet_rt.worksheets()] else sheet_rt.add_worksheet("Backup", rows="1000", cols="60")
    ws_rt.clear()
    set_with_dataframe(ws_rt, talabat_rest)
    print("Rest of branches spreadsheet updated.")

    return talabat_all, talabat_first3, talabat_rest

if __name__ == "__main__":
    all_df, first3_df, rest_df = run_all_and_push()
