# save_as: seoudi_products_to_sheets.py
# Requirements:
#   pip install requests gspread google-auth streamlit
#
# Usage: run in Streamlit environment so st.secrets["SERVICE_ACCOUNT_DICT"] exists,
# or adapt SERVICE_ACCOUNT_DICT assignment to load from a file/env if needed.

import requests
import json
import time
from datetime import datetime
try:
    # Python 3.9+ standard zoneinfo
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import streamlit as st  # you already use st.secrets per your request

import gspread
from google.oauth2.service_account import Credentials

# ---------------------------
# Your GraphQL request (unchanged core)
# ---------------------------
url = "https://mcprod.seoudisupermarket.com/graphql"

query = """
query Products($page:Int $pageSize:Int $search:String $filter:ProductAttributeFilterInput={} $sort:ProductAttributeSortInput={}) {
  connection: products(currentPage: $page pageSize: $pageSize filter: $filter search: $search sort: $sort) {
    total_count
    aggregations { ...ProductAggregation }
    page_info { ...PageInfo }
    nodes: items { ...ProductCard }
  }
}
fragment ProductAggregation on Aggregation {
  attribute_code
  label
  count
  options { label count value }
}
fragment PageInfo on SearchResultPageInfo {
  total_pages
  current_page
  page_size
}
fragment ProductCard on ProductInterface {
  __typename
  id
  name
  sku
  special_from_date
  special_price
  special_to_date
  new_from_date
  new_to_date
  only_x_left_in_stock
  url_key
  weight_increment_step
  weight_base_unit
  brand { name url_key }
  categories { url_path name level max_allowed_qty }
  thumbnail { url label }
  price_range {
    maximum_price {
      final_price { value }
      regular_price { value }
    }
  }
  stockQtyTerm { max_sale_qty min_sale_qty }
}
"""

variables = {
    "page": 1,
    "pageSize": 500,
    "sort": {"position": "ASC"},
    "filter": {"category_uid": {"eq": "Mjg5NA=="}}
}

headers = {
    "accept": "*/*",
    "accept-language": "ar-EG,ar;q=0.9,en;q=0.8",
    "content-type": "application/json",
    "origin": "https://seoudisupermarket.com",
    "priority": "u=1, i",
    "referer": "https://seoudisupermarket.com/",
    "sec-ch-ua": "\"Not;A=Brand\";v=\"99\", \"Brave\";v=\"139\", \"Chromium\";v=\"139\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "sec-gpc": "1",
    "sourcecode": "18",
    "store": "ar_EG",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
}

# ---------------------------
# Make request
# ---------------------------
response = requests.post(url, headers=headers, json={"query": query, "variables": variables})

# Stop if non-JSON / bad response
try:
    data = response.json()
except ValueError:
    raise SystemExit(f"Response not JSON. HTTP {response.status_code}: {response.text}")

# Basic GraphQL error handling
if "errors" in data:
    print("GraphQL returned errors:")
    print(json.dumps(data["errors"], indent=2, ensure_ascii=False))
    # You might want to raise or continue depending on your workflow
    # raise SystemExit("GraphQL errors returned")

nodes = data.get("data", {}).get("connection", {}).get("nodes", [])
total_count = data.get("data", {}).get("connection", {}).get("total_count", len(nodes))

print(f"Returned nodes: {len(nodes)}, reported total_count: {total_count}")

# ---------------------------
# Extract lists (safer: iterate nodes rather than range(total_count))
# ---------------------------

skus = []
names = []
prices = []

for n in nodes:
    # some items may be missing fields; use .get
    sku = n.get("sku") or ""
    url_key = n.get("name")
    weight = n.get("weight_increment_step")
    if weight == None:
        weight = ""
    else:
        weight = weight
    unit = n.get("weight_base_unit")
    if unit == None:
        unit = ""
    else:
        unit = unit
    # convert url_key to readable name (replace dashes)
    name = url_key + " " + str(weight) + " " + str(unit)
    # price may be missing; use get chain safely
    price = None
    try:
        price = n.get("price_range", {}).get("maximum_price", {}).get("final_price", {}).get("value")
        if weight != None:
            price = round(price * float(weight), 2)
        else:
            price = price
    except Exception:
        price = None

    skus.append(int(sku))
    names.append(name)
    prices.append(price)

# Quick sanity print
print(f"Collected {len(skus)} SKUs, {len(names)} names, {len(prices)} prices")


# ---------------------------
# Google Sheets: write the 3 lists into the provided sheet
# ---------------------------

# 1) Spreadsheet ID from your URL:
SPREADSHEET_ID = "1bHsZvDJQ1U-V3yPalU2gKNRqLOyjtFP509NpHSVj6_k"  # extracted from your URL
WORKSHEET_TITLE = "Seoudi"

# 2) Load service account dict from Streamlit secrets (as you requested)
#    your existing line:
SERVICE_ACCOUNT_DICT = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])

# 3) Authorize gspread using google.oauth2.service_account
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_DICT, scopes=SCOPES)
gc = gspread.authorize(credentials)

# 4) Open spreadsheet
sh = gc.open_by_key(SPREADSHEET_ID)

# 5) Get or create the worksheet
try:
    worksheet = sh.worksheet(WORKSHEET_TITLE)
except gspread.exceptions.WorksheetNotFound:
    # create with a few extra rows/cols
    rows_guess = max(100, len(skus) + 5)
    worksheet = sh.add_worksheet(title=WORKSHEET_TITLE, rows=str(rows_guess), cols="4")

# 6) Prepare rows to upload
# Header: sku, name, price, last_updated
header = ["sku", "name", "price", "last_updated"]

# last_updated in Egypt (Africa/Cairo)
def now_egypt_iso():
    try:
        if ZoneInfo is not None:
            tz = ZoneInfo("Africa/Cairo")
            return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        else:
            # fallback to naive local time if zoneinfo isn't available
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

last_updated_value = now_egypt_iso()

# Build rows list of lists
rows = []
for sku, name, price in zip(skus, names, prices):
    # price might be None, convert to empty string or keep as-is
    rows.append([sku, name, price if price is not None else "", last_updated_value])

# 7) Clear existing content and update sheet
worksheet.clear()
# Write header + rows in one update for efficiency
worksheet.update([header] + rows)  # gspread expects a list-of-rows (2D list)

print(f"Wrote {len(rows)} rows to spreadsheet '{SPREADSHEET_ID}' worksheet '{WORKSHEET_TITLE}'.")

# If you want to also print the top-level keys for quick inspection:
print("Top-level response keys:", list(data.keys()))
if "data" in data:
    print("GraphQL 'data' keys:", list(data["data"].keys()))
