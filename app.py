import streamlit as st
import pandas as pd
from handler import clean_columns, process_shipping_data, get_msc_group_excel, map_tracking_dates_to_main_df
from track import track_msc_shipments

st.set_page_config(page_title="Shipping Line Tracker", layout="centered")
st.title("📦 Shipping Line Tracker")
st.markdown("Upload an Excel file, and we'll extract rows related to **MSC** shipping lines.")

# Initialize session state
if 'raw_df' not in st.session_state:
    st.session_state.raw_df = None
if 'output_excel' not in st.session_state:
    st.session_state.output_excel = None
if 'msc_count' not in st.session_state:
    st.session_state.msc_count = 0

# Step 1: Upload file
uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"])

if uploaded_file:
    try:
        raw_df = pd.read_excel(uploaded_file)
        st.session_state.raw_df = raw_df
        st.success("✅ File uploaded and cleaned successfully.")
    except Exception as e:
        st.error(f"⚠️ Error reading file: {str(e)}")

# Step 2: Fetch Info button
if st.session_state.raw_df is not None:
    if st.button("🔍 Fetch MSC Tracking Info"):
        try:
            cleaned_df = clean_columns(st.session_state.raw_df)
            processed_df = process_shipping_data(cleaned_df)
            group_df = get_msc_group_excel(processed_df)
            tracking_df = track_msc_shipments(group_df)
            mapped_df = map_tracking_dates_to_main_df(cleaned_df, tracking_df)

            st.session_state.output_excel, st.session_state.msc_count = mapped_df
            st.success(f"✅ Tracking info fetched for {st.session_state.msc_count} Bill of Lading related to MSC.")

        except Exception as e:
            st.error(f"⚠️ Error during tracking fetch: {str(e)}")

# Step 3: Show Download Button
if st.session_state.output_excel:
    st.download_button(
        label="📥 Download MSC Filtered Excel",
        data=st.session_state.output_excel,
        file_name="msc_filtered_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
