import json, gspread, streamlit as st
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo
from datetime import datetime

SA = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])
pk = SA.get("private_key", "")
pk = pk.replace("\\\\n", "\n").replace("\\n", "\n")
SA["private_key"] = pk
creds = Credentials.from_service_account_info(SA, scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
gc = gspread.authorize(creds)
src = gc.open_by_key("1yp-1Zswwwf3ZjNWGrks0R3OFqZWlF30jwAwb-PZV1XM").worksheet("Summary")
rows = src.get_all_records()
out = [[r.get("Barcode",""), r.get("product_name_ar",""), r.get("Current_Talabat_price",""), r.get("elnour_current_price",""), r.get("seoudi_current_price",""), datetime.now(ZoneInfo("Africa/Cairo")).date().isoformat()] for r in rows]
if out:
    gc.open_by_key("1bHsZvDJQ1U-V3yPalU2gKNRqLOyjtFP509NpHSVj6_k").worksheet("comparison").append_rows(out, value_input_option="USER_ENTERED")
