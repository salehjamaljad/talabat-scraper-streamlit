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

def fetch_and_process(branch):
    """
    Fetch products matching 'khodar.com' for a branch UUID,
    filter to khodar_skus, enrich, inject missing SKUs.
    Returns the branch DataFrame with SKU, title, category,
    {branch}_stock, {branch}_price, {branch}_last_updated.
    """
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

    # Build DataFrame
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

    # Keep only configured SKUs
    df = df[df["sku"].isin(khodar_skus)].copy()
    # Overwrite title and category
    df["title"] = df["sku"].apply(lambda s: khodar_skus[s]["title"])
    df["category"] = df["sku"].apply(lambda s: khodar_skus[s]["category"])

    # Ensure all SKUs present
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


if __name__ == "__main__":
    # Process first three branches
    dfs = [fetch_and_process(branch) for branch in branches_uuids[:3]]

    # Merge all branch DataFrames on 'sku'
    alex = dfs[0]
    for df_branch in dfs[1:]:
        alex = alex.merge(df_branch, on=["sku", "title", "category"], how="outer")

    # Consolidate price across branch-specific price columns
    price_cols = [col for col in alex.columns if col.endswith("_price")]
    alex["price"] = alex[price_cols].bfill(axis=1).iloc[:, 0]

    # Consolidate last_updated across branch-specific columns
    date_cols = [col for col in alex.columns if col.endswith("_last_updated")]
    alex["last_updated"] = pd.to_datetime(alex[date_cols].bfill(axis=1).iloc[:, 0])

    # Fill missing stocks with 0 and calculate total stock
    stock_cols = [col for col in alex.columns if col.endswith("_stock")]
    for col in stock_cols:
        alex[col] = alex[col].fillna(0).astype(int)
    alex["total stock"] = alex[stock_cols].sum(axis=1)

    # Select final columns
    final_cols = ["sku", "title", "price"] + stock_cols + ["total stock", "last_updated", "category"]
    alexandria = alex[final_cols]

    # Authenticate and write to Google Sheet
    SERVICE_ACCOUNT_DICT = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_DICT, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key("1d3oDBdu8SqnBlaFDrBDL2lEwe5F9f4RL7RTzK_SRW-4")
    try:
        ws = sheet.worksheet("Backup")
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title="Backup", rows="1000", cols="20")

    set_with_dataframe(ws, alexandria)
    print("Alexandria sheet updated successfully.")
