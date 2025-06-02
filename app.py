# app.py
import streamlit as st
from talabat_scraper import run_scraper

st.set_page_config(page_title="Talabat Scraper", page_icon="🛒")

st.title("Talabat Daily Scraper")
st.write("Click the button below to run the Talabat scraping logic.")

# Place a button. When clicked, call run_scraper() and print each step.
if st.button("Run Talabat Scraper Now"):
    with st.spinner("Running Talabat scraper…"):
        try:
            run_scraper()
            st.success("✅ Talabat scraping completed. Check the logs above for step‐by‐step output.")
        except Exception as e:
            st.error(f"❌ An error occurred: {e}")
