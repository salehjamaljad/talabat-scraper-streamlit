import random
import requests
import json
import time
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import re
import gspread
from gspread_dataframe import set_with_dataframe
from openpyxl import load_workbook
import pandas as pd
import datetime
import gspread
import pandas as pd
import numpy as np
import datetime
import threading
import os
import pytz
import mimetypes
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import streamlit as st
def run_scraper():
    def random_delay(min_ms, max_ms):
        delay = (min_ms + (max_ms - min_ms) * random.random()) / 1000.0
        time.sleep(delay)

    category_names = {
        "%D9%81%D9%88%D8%A7%D9%83%D9%87": "فواكه",
        "%D8%AE%D8%B6%D8%B1%D9%88%D8%A7%D8%AA": "خضروات",
        "%D8%A3%D8%B9%D8%B4%D8%A7%D8%A8-%D9%88-%D9%88%D8%B1%D9%82%D9%8A%D8%A7%D8%AA": "أعشاب وورقيات",
        "%D8%AA%D9%85%D8%B1-%D9%88%D9%81%D9%88%D8%A7%D9%83%D9%87-%D9%85%D8%AC%D9%81%D9%81%D8%A9": "تمور وفواكه مجففة"
    }

    branch_info = [
    { "name": "Ibrahimia", "id": "650267", "aid": "9958" },
    { "name": "Wenget", "id": "725493", "aid": "9981" },
    { "name": "Sidibeshr", "id": "650266", "aid": "8328" },
    { "name": "Agouza", "id": "613174", "aid": "8211" },
    { "name": "AinShams", "id": "655461", "aid": "7830" },
    { "name": "Al-Manial", "id": "619847", "aid": "7583" },
    { "name": "Asyut", "id": "668930", "aid": "7239" },
    { "name": "Bright stars", "id": "750753", "aid": "7935" },
    { "name": "City Stars", "id": "620008", "aid": "7603" },
    { "name": "Dokki", "id": "613174", "aid": "8212" },
    { "name": "El Sayida Zeinab", "id": "619850", "aid": "7761" },
    { "name": "ElDaher", "id": "620009", "aid": "7575" },
    { "name": "Faisal", "id": "725603", "aid": "10013" },
    { "name": "First Mall", "id": "620007", "aid": "7848" },
    { "name": "First Settlement", "id": "686917", "aid": "7845" },
    { "name": "Fount Mall", "id": "620014", "aid": "9950" },
    { "name": "Golden Square", "id": "745090", "aid": "9289" },
    { "name": "Hadaeq Al Qubbah", "id": "620402", "aid": "7699" },
    { "name": "Hadayek October", "id": "736540", "aid": "7990" },
    { "name": "Haram Trsa", "id": "619838", "aid": "7675" },
    { "name": "Harm Hadek", "id": "620010", "aid": "7949" },
    { "name": "Heliopolis", "id": "619849", "aid": "7656" },
    { "name": "Helwan", "id": "619842", "aid": "7596" },
    { "name": "Heliopolis sheraton", "id": "760059", "aid": "7656" },
    { "name": "Ismailia", "id": "662683", "aid": "8531" },
    { "name": "Madinaty", "id": "644838", "aid": "7632" },
    { "name": "Madinaty Craft", "id": "717767", "aid": "7628" },
    { "name": "Maadi Corniche", "id": "619848", "aid": "7965" },
    { "name": "Maadi Lasleky", "id": "613172", "aid": "7882" },
    { "name": "Mansoura", "id": "650265", "aid": "8713" },
    { "name": "Mansoura gomhoreya", "id": "752616", "aid": "8387" },
    { "name": "Midan Lubnan", "id": "613177", "aid": "7533" },
    { "name": "Mokatam", "id": "615751", "aid": "10102" },
    { "name": "Mokatam hadaba", "id": "745095", "aid": "7433" },
    { "name": "Nasr City", "id": "619844", "aid": "7744" },
    { "name": "Nasr City Hay Asher", "id": "717765", "aid": "7743" },
    { "name": "Obour", "id": "650270", "aid": "8036" },
    { "name": "October", "id": "618524", "aid": "10045" },
    { "name": "Palm Hills", "id": "669870", "aid": "8182" },
    { "name": "Ports Said", "id": "661094", "aid": "8836" },
    { "name": "Rehab", "id": "613175", "aid": "7600" },
    { "name": "Rehab Two", "id": "756215", "aid": "7743" },
    { "name": "Shobra", "id": "650268", "aid": "7633" },
    { "name": "Shrouk", "id": "650269", "aid": "7816" },
    { "name": "Shrouk 2", "id": "758922", "aid": "7822" },
    { "name": "TAGMOE MAHKMA", "id": "726868", "aid": "7870" },
    { "name": "Tanta", "id": "650264", "aid": "7084" },
    { "name": "Zagazig", "id": "662546", "aid": "7087" },
    { "name": "Zahraa El Maadi", "id": "613176", "aid": "8017" },
    { "name": "Zahraa El Maadi Two", "id": "613176", "aid": "7943" },
    { "name": "Zayed", "id": "620011", "aid": "8069" }
    ]


    branch_errors = []
    categories_errors = []



    def scraper(branch_id, category_id, category_name, max_retries=3):
        url = f"https://www.talabat.com/nextApi/groceries/stores/{branch_id}/feed?countryId=9&categorySlug=%D8%A7%D9%84%D9%81%D9%88%D8%A7%D9%83%D9%87-%D9%88%D8%A7%D9%84%D8%AE%D8%B6%D8%B1%D9%88%D8%A7%D8%AA&subCategorySlug={category_id}"
        branch_name = next((b["name"] for b in branch_info if b["id"] == branch_id), "Branch ID not found")
        branch_aid = next((b["aid"] for b in branch_info if b["id"] == branch_id), "Branch AID not found")

        user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
        ]


        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ar-KW",
            "appbrand": "1",
            "priority": "u=1, i",
            "sec-ch-ua": '"Chromium";v="133", "Not(A:Brand";v="99", "Google Chrome";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sourceapp": "web",
            "x-device-source": "0",
            "Referer": "https://www.talabat.com/ar/egypt/grocery/650265/talabat-mart/",
            "User-Agent": random.choice(user_agents)
        }

        retries = 0
        while retries < max_retries:
            
            response = requests.get(url, headers=headers)
            st.info(f'{response.status_code} for category {category_name}')
            if response.status_code == 404:
                globals()[f'stock_amounts_{branch_name}_{category_name}'] = []

                globals()[f'titles_{branch_name}_{category_name}'] = []

                globals()[f'prices_{branch_name}_{category_name}'] = []

                # Check consistency
                st.info(f"\nItems Count for branch {branch_name} category {category_name} - Titles: {len(globals()[f'titles_{branch_name}_{category_name}'])}, Prices: {len(globals()[f'prices_{branch_name}_{category_name}'])}, Stocks: {len(globals()[f'stock_amounts_{branch_name}_{category_name}'])}")
                break
            elif response.status_code == 200:
                # Extract data

                globals()[f'stock_amounts_{branch_name}_{category_name}'] = list(map(int, re.findall(r'"stockAmount":\s*(\d+)', response.text)))

                globals()[f'titles_{branch_name}_{category_name}'] = re.findall(r'"title":\s*"([^"]+)"', response.text)[1:]

                globals()[f'prices_{branch_name}_{category_name}'] = list(map(float, re.findall(r'"price":\s*(\d+\.?\d*)', response.text)))

                # st.info results
                for title, price, stock in zip(globals()[f'titles_{branch_name}_{category_name}'], globals()[f'prices_{branch_name}_{category_name}'], globals()[f'stock_amounts_{branch_name}_{category_name}']):
                    st.info(f"{title}: {price} EGP (Stock: {stock})")

                # Check consistency
                st.info(f"\nItems Count for branch {branch_name} category {category_name} - Titles: {len(globals()[f'titles_{branch_name}_{category_name}'])}, Prices: {len(globals()[f'prices_{branch_name}_{category_name}'])}, Stocks: {len(globals()[f'stock_amounts_{branch_name}_{category_name}'])}")
                break
            elif "Cannot read properties of undefined" in response.text:
                st.info(f"Encountered error for URL {url}. Performing secondary request with aid: {branch_aid}")

                # Construct and perform the secondary request
                secondary_url = f"https://www.talabat.com/ar/egypt/grocery/{branch_id}/talabat-mart/fruit-veg/fresh-fruit?aid={branch_aid}"
                requests.get(secondary_url, headers=headers)

                st.info(f"Secondary request complete. Retrying original request for {url}")

                # Retry the original request after a short delay
                time.sleep(2)  # Give some time for the server to process the secondary request
                response = requests.get(url, headers=headers)
            elif ConnectionResetError:
                random_delay(3000,5000)
                response = requests.get(url, headers=headers)
            elif ConnectionError:
                random_delay(3000,5000)
                response = requests.get(url, headers=headers)
            else:
                st.info(f"Failed to fetch data. Status code: {response.status_code}, {branch_id}")
                st.info(response.text)
                retries += 1
                headers["User-Agent"] = random.choice(user_agents)
                random_delay(7500, 15000)
                if retries == max_retries:
                    st.info("Max retries reached. Skipping...")
                    branch_errors.append(branch_id)
                    categories_errors.append(category_id)


    for branch in branch_info:
        random_delay(7500, 15000)
        threads = []
        st.info(f"Starting categories for branch {branch['name']}")

        # Start threads for the 4 categories concurrently
        for category_id, category_name in category_names.items():
            t = threading.Thread(target=scraper, args=(branch["id"], category_id, category_name))
            threads.append(t)
            t.start()

        # Wait for all category threads to finish before moving to the next branch
        for t in threads:
            t.join()

        st.info(f"Finished categories for branch {branch['name']}")


    SERVICE_ACCOUNT_DICT = json.loads(st.secrets["SERVICE_ACCOUNT_DICT"])

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Authenticate using the service account credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_DICT, scope)
    client = gspread.authorize(credentials)


    cairo_tz = pytz.timezone('Africa/Cairo')
    now = datetime.datetime.now(pytz.utc).astimezone(cairo_tz).strftime("%Y-%m-%d %H:%M:%S")

    # Create the folder if it doesn't exist
    folder_name = f"talabat_{now.replace(':', '-')}"
    os.makedirs(folder_name, exist_ok=True)

    # Loop through each branch
    for branch in branch_info:
        branch_name = branch["name"]  # lowercase for consistency
        branch_dfs = {}

        for encoded_cat, category_name in category_names.items():
            # Generate list variable names
            titles_var = f"titles_{branch_name}_{category_name}"
            prices_var = f"prices_{branch_name}_{category_name}"
            stock_var = f"stock_amounts_{branch_name}_{category_name}"

            # Retrieve lists from globals() if they exist
            titles = globals().get(titles_var, [])
            prices = globals().get(prices_var, [])
            stock_amounts = globals().get(stock_var, [])

            # Determine column name dynamically
            stock_col_name = f"{branch_name}_stock_amount"

            # Validate lengths
            if len(titles) == len(prices) == len(stock_amounts) and len(titles) > 0:
                # Create DataFrame
                globals()[f'df_{branch_name}_{category_name}'] = pd.DataFrame({
                    "title": titles,
                    'price': prices,
                    stock_col_name: stock_amounts,
                    "last_updated": now
                })
            else:
                # Create a placeholder row if lists don't match or are missing
                globals()[f'df_{branch_name}_{category_name}'] = pd.DataFrame([{
                    "title": "",
                    'price': "",
                    stock_col_name: "",
                    "last_updated": now
                }])

            # Store DataFrame
            branch_dfs[category_name] = globals()[f'df_{branch_name}_{category_name}']

        # Save DataFrames to Excel inside the folder
        file_name = os.path.join(folder_name, f"{branch_name}_{now.replace(':', '-')}.xlsx")
        with pd.ExcelWriter(file_name) as writer:
            for category_name, df in branch_dfs.items():
                df.to_excel(writer, sheet_name=category_name, index=False)

        st.info(f"Saved {file_name} successfully!")

    # Merge DataFrames for each category across all Alexandria branches
    for category_name in category_names.values():
        # Initialize merged DataFrame as None
        merged = None

        # Iterate through all branches and merge them one by one
        for branch in branch_info[:3]:
            branch_name = branch["name"]
            df = globals().get(f'df_{branch_name}_{category_name}')

            if df is not None:
                if merged is None:
                    merged = df.rename(columns={
                        "price": "price",
                        "stock_amount": f"{branch_name}_stock_amount",
                        "last_updated": f"{branch_name}_last_updated"
                    })
                else:
                    merged = merged.merge(
                        df.rename(columns={
                            "price": f"{branch_name}_price",
                            "stock_amount": f"{branch_name}_stock_amount",
                            "last_updated": f"{branch_name}_last_updated"
                        }),
                        on="title",
                        how="outer"
                    )

        if merged is not None:
            # Remove rows where 'title' is blank (not just NaN)
            merged = merged[merged["title"].str.strip() != ""]

            # Combine price columns by taking the first available price
            price_cols = [col for col in merged.columns if "price" in col]
            merged["price"] = merged[price_cols].bfill(axis=1).iloc[:, 0]

            # Combine last_updated columns
            last_updated_cols = [col for col in merged.columns if "last_updated" in col]
            merged["last_updated"] = merged[last_updated_cols].bfill(axis=1).ffill(axis=1).iloc[:, 0]

            # Fill missing stock amounts with 0
            stock_cols = [col for col in merged.columns if "stock_amount" in col]
            for col in stock_cols:
                merged[col] = merged[col].fillna(0)

            # Add total_stock column by summing all stock_amount columns
            merged["total_stock"] = merged[stock_cols].sum(axis=1)

            # Select the final columns (price, total_stock, stock amounts, last_updated)
            final_columns = ["title", "price", "total_stock"] + stock_cols + ["last_updated"]
            merged = merged[final_columns]

            # Save the merged DataFrame for Alexandria
            globals()[f'df_alexandria_{category_name}'] = merged

    file_name = os.path.join(folder_name, f"alexandria_{now.replace(':', '-')}.xlsx")

    # Save the Alexandria DataFrames to the Excel file
    with pd.ExcelWriter(file_name) as writer:
        for category_name in category_names.values():
            df = globals().get(f'df_alexandria_{category_name}')
            if df is not None:
                df.to_excel(writer, sheet_name=category_name, index=False)

    st.info(f"File saved successfully at {file_name}")


    # Merge DataFrames for each category across all Cairo branches
    for category_name in category_names.values():
        # Initialize merged DataFrame as None
        merged = None

        # Iterate through all branches and merge them one by one
        for branch in branch_info[3:]:
            branch_name = branch["name"]
            df = globals().get(f'df_{branch_name}_{category_name}')

            if df is not None:
                if merged is None:
                    merged = df.rename(columns={
                        "price": "price",
                        "stock_amount": f"{branch_name}_stock_amount",
                        "last_updated": f"{branch_name}_last_updated"
                    })
                else:
                    merged = merged.merge(
                        df.rename(columns={
                            "price": f"{branch_name}_price",
                            "stock_amount": f"{branch_name}_stock_amount",
                            "last_updated": f"{branch_name}_last_updated"
                        }),
                        on="title",
                        how="outer"
                    )

        if merged is not None:
            # Remove rows where 'title' is blank (not just NaN)
            merged = merged[merged["title"].str.strip() != ""]

            # Combine price columns by taking the first available price
            price_cols = [col for col in merged.columns if "price" in col]
            merged["price"] = merged[price_cols].bfill(axis=1).iloc[:, 0]

            # Combine last_updated columns
            last_updated_cols = [col for col in merged.columns if "last_updated" in col]
            merged["last_updated"] = merged[last_updated_cols].bfill(axis=1).ffill(axis=1).iloc[:, 0]

            # Fill missing stock amounts with 0
            stock_cols = [col for col in merged.columns if "stock_amount" in col]
            for col in stock_cols:
                merged[col] = merged[col].fillna(0)

            # Add total_stock column by summing all stock_amount columns
            merged["total_stock"] = merged[stock_cols].sum(axis=1)
            
            # Select the final columns (price, stock amounts, last_updated)
            final_columns = ["title", "price", "total_stock"] + stock_cols + ["last_updated"]
            merged = merged[final_columns]

            # Save the merged DataFrame for Cairo
            globals()[f'df_cairo_{category_name}'] = merged

    file_name = os.path.join(folder_name, f"cairo_{now.replace(':', '-')}.xlsx")

    # Save the Cairo DataFrames to the Excel file
    with pd.ExcelWriter(file_name) as writer:
        for category_name in category_names.values():
            df = globals().get(f'df_cairo_{category_name}')
            if df is not None:
                df.to_excel(writer, sheet_name=category_name, index=False)

    st.info(f"File saved successfully at {file_name}")


    # Merge DataFrames for each category across all talabat branches
    for category_name in category_names.values():
        # Initialize merged DataFrame as None
        merged = None

        # Iterate through all branches and merge them one by one
        for branch in branch_info:
            branch_name = branch["name"]
            df = globals().get(f'df_{branch_name}_{category_name}')

            if df is not None:
                if merged is None:
                    merged = df.rename(columns={
                        "price": "price",
                        "stock_amount": f"{branch_name}_stock_amount",
                        "last_updated": f"{branch_name}_last_updated"
                    })
                else:
                    merged = merged.merge(
                        df.rename(columns={
                            "price": f"{branch_name}_price",
                            "stock_amount": f"{branch_name}_stock_amount",
                            "last_updated": f"{branch_name}_last_updated"
                        }),
                        on="title",
                        how="outer"
                    )

        if merged is not None:
            # Remove rows where 'title' is blank (not just NaN)
            merged = merged[merged["title"].str.strip() != ""]

            # Combine price columns by taking the first available price
            price_cols = [col for col in merged.columns if "price" in col]
            merged["price"] = merged[price_cols].bfill(axis=1).iloc[:, 0]

            # Combine last_updated columns
            last_updated_cols = [col for col in merged.columns if "last_updated" in col]
            merged["last_updated"] = merged[last_updated_cols].bfill(axis=1).ffill(axis=1).iloc[:, 0]

            # Fill missing stock amounts with 0
            stock_cols = [col for col in merged.columns if "stock_amount" in col]
            for col in stock_cols:
                merged[col] = merged[col].fillna(0)
                
            # Add total_stock column by summing all stock_amount columns
            merged["total_stock"] = merged[stock_cols].sum(axis=1)

            # Select the final columns (price, total_stock, stock amounts, last_updated)
            final_columns = ["title", "price", "total_stock"] + stock_cols + ["last_updated"]
            merged = merged[final_columns]

            # Save the merged DataFrame for talabat
            globals()[f'df_talabat_{category_name}'] = merged

    file_name = os.path.join(folder_name, f"talabat_{now.replace(':', '-')}.xlsx")

    # Save the talabat DataFrames to the Excel file
    with pd.ExcelWriter(file_name) as writer:
        for category_name in category_names.values():
            df = globals().get(f'df_talabat_{category_name}')
            if df is not None:
                df.to_excel(writer, sheet_name=category_name, index=False)

    st.info(f"File saved successfully at {file_name}")




    SCOPES = ["https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    # Authenticate using google-auth
    try:
        credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_DICT, scopes=SCOPES)
    except Exception:
        SERVICE_ACCOUNT_DICT = json.loads(os.environ["SERVICE_ACCOUNT_DICT"])
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_DICT, SCOPES)
    drive_service = build('drive', 'v3', credentials=credentials)


    # Folder where you want to upload files
    parent_folder_id = '1OYDl964QyO1tLIDfOJf1_3f3nJUop7YV'  # The folder ID you want to upload into



    # Get the path to the folder you want to upload (located in the same directory as the code)
    local_folder_path = os.path.join(os.getcwd(), folder_name)

    # Function to upload a file to Google Drive
    def upload_file(file_path, parent_folder_id):
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [parent_folder_id]
        }
        mime_type, _ = mimetypes.guess_type(file_path)
        media = MediaFileUpload(file_path, mimetype=mime_type)

        request = drive_service.files().create(
            media_body=media,
            body=file_metadata,
            fields='id'
        )
        response = request.execute()
        st.info(f"Uploaded {file_path} with ID {response['id']}")

    # Function to upload an entire folder to Google Drive
    def upload_folder(local_folder_path, parent_folder_id):
        # Create a folder in Google Drive
        folder_metadata = {
            'name': os.path.basename(local_folder_path),
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder['id']
        st.info(f"Created folder with ID {folder_id} on Google Drive.")

        # Upload each file in the local folder into the created Google Drive folder
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                upload_file(file_path, folder_id)
                random_delay(1000,3000)

    # Upload the folder and its contents
    upload_folder(local_folder_path, parent_folder_id)



    spreadsheet = client.open_by_key("1cbhFF2daE7iUe0vlebIwohQN3UoxsZWY4I_blqr_8rk")

    for category in category_names.values():
        # Reset the index
        globals()[f'df_cairo_{category}'].reset_index(drop=True, inplace=True)

        # Open the worksheet
        worksheet = spreadsheet.worksheet(category)

        # Clear the sheet before uploading the new data
        worksheet.clear()

        # Upload the updated DataFrame to the sheet
        worksheet.update(
            [globals()[f'df_cairo_{category}'].columns.values.tolist()] +
            globals()[f'df_cairo_{category}'].values.tolist()
        )
    st.info("Cairo Done")
    random_delay(3000,7500)


    spreadsheet = client.open_by_key("1d3oDBdu8SqnBlaFDrBDL2lEwe5F9f4RL7RTzK_SRW-4")

    for category in category_names.values():
        # Reset the index
        globals()[f'df_alexandria_{category}'].reset_index(drop=True, inplace=True)

        # Open the worksheet
        worksheet = spreadsheet.worksheet(category)

        # Clear the sheet before uploading the new data
        worksheet.clear()

        # Upload the updated DataFrame to the sheet
        worksheet.update(
            [globals()[f'df_alexandria_{category}'].columns.values.tolist()] +
            globals()[f'df_alexandria_{category}'].values.tolist()
        )
    st.info("Alexandria Done")
    random_delay(3000,7500)

    spreadsheet = client.open_by_key("1bHsZvDJQ1U-V3yPalU2gKNRqLOyjtFP509NpHSVj6_k")

    for category in category_names.values():
        # Reset the index
        globals()[f'df_talabat_{category}'].reset_index(drop=True, inplace=True)

        # Open the worksheet
        worksheet = spreadsheet.worksheet(category)

        # Clear the sheet before uploading the new data
        worksheet.clear()

        # Upload the updated DataFrame to the sheet
        worksheet.update(
            [globals()[f'df_talabat_{category}'].columns.values.tolist()] +
            globals()[f'df_talabat_{category}'].values.tolist()
        )

    st.info("Talabat Done")


if __name__ == "__main__":
    run_scraper()
