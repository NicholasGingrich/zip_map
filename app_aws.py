from io import BytesIO
import streamlit as st
import boto3
import time
from datetime import datetime
import pandas as pd


# ----------------------------
# Initialize Streamlit State
# ----------------------------
if "processing" not in st.session_state:
    st.session_state.processing = False

if "png_bytes" not in st.session_state:
    st.session_state.png_bytes = None

if "csv_bytes" not in st.session_state:
    st.session_state.csv_bytes = None

# -----------------------------
# Configuration
# -----------------------------
AWS_REGION = "us-east-1"
S3_BUCKET = "zip-map-bucket"
UPLOAD_PREFIX = "uploads/"
RESULTS_PREFIX = "results/"

s3 = boto3.client("s3", region_name=AWS_REGION)

# -----------------------------
# Page Setup
# -----------------------------
st.set_page_config(layout="wide")
st.title("ZIP Code Coverage Map")

# -----------------------------
# User Inputs
# -----------------------------
zip_col_label = st.text_input(
    "Zip Code Column Label",
    help="Enter the name of the column in your Excel file that contains ZIP codes."
)
value_col_label = st.text_input(
    "Value Column Label",
    help="Enter the name of the column in your Excel file that contains the values to map."
)
map_title = st.text_input(
    "Map Title (Optional)",
    help="Optional map title for display purposes."
)
excel_file = st.file_uploader("Upload Excel File", type=["xlsx"])

with st.expander(label="Advanced Options"):
    auto_assign_zipcodes = st.checkbox(label="Auto-Assign Missing Zip Codes", value=True)

generate_button = st.button(
    "Generate Map",
    disabled=st.session_state.processing
)

# -----------------------------
# Helpers
# -----------------------------
def upload_excel_to_s3(file, zip_col, value_col, map_title):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = file.name.replace(".xlsx", f"_{timestamp}.xlsx").replace(" ", "_")
    s3_key = f"{UPLOAD_PREFIX}{safe_name}"

    s3.upload_fileobj(
        file,
        S3_BUCKET,
        s3_key,
        ExtraArgs={
            "Metadata": {
                "zip_col": zip_col,
                "value_col": value_col,
                "map_title": map_title or "",
                "auto_assign_zipcodes": str(auto_assign_zipcodes)
            }
        }
    )

    return s3_key


def check_s3_file_exists(key):
    """Check if file exists in S3"""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def download_s3_file_to_bytes(key):
    """Download S3 object into bytes"""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read()

# -----------------------------
# Main Logic
# -----------------------------
if generate_button and not st.session_state.processing:
    st.session_state.processing = True
    if not excel_file:
        st.session_state.processing = False
        st.error("Please upload an Excel file.")
        st.stop()
    if not zip_col_label:
        st.session_state.processing = False
        st.error("Please enter a Zip Code column label.")
        st.stop()
    if not value_col_label:
        st.session_state.processing = False
        st.error("Please enter a Value column label.")
        st.stop()

    # Upload Excel to S3
    with st.spinner("Uploading File For Processing..."):
        excel_s3_key = upload_excel_to_s3(
            excel_file,
            zip_col_label,
            value_col_label,
            map_title
        )

    # Poll for result
    result_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", ".png")
    unassigned_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", "_unassigned.csv")
    progress_text = st.empty()
    progress_bar = st.progress(0)

    poll_interval = 1  
    timeout = 900 
    elapsed = 0

    while elapsed < timeout:
        if check_s3_file_exists(result_s3_key) and check_s3_file_exists(unassigned_s3_key):
            progress_bar.progress(1.0)
            progress_text.text("Done")
            break
        time.sleep(poll_interval)
        elapsed += poll_interval
        progress_bar.progress(min(elapsed / timeout, 0.99))
        progress_text.text(f"Generating Map. Do not close or refresh the page... {elapsed}s elapsed")

    else:
        st.session_state.processing = False
        st.error("Timed out waiting for Lambda to generate the map.")
        st.stop()

    # Download result and display
    png_bytes = download_s3_file_to_bytes(result_s3_key)
    csv_bytes = download_s3_file_to_bytes(unassigned_s3_key)

    st.session_state.png_bytes = png_bytes
    st.session_state.csv_bytes = csv_bytes

    st.success("Map ready for download")
    st.session_state.processing = False

if st.session_state.png_bytes is not None:
    st.image(st.session_state.png_bytes)

    # Download button
    st.download_button(
        label="Download Map as PNG",
        data=st.session_state.png_bytes,
        file_name="coverage_map.png",
        mime="image/png",
    )

if st.session_state.csv_bytes is not None:
    with st.expander(label="View Unassigned ZIP Codes"):
        st.write("The following zipcodes were missing from uploaded excel file. If the Auto-Assign checkbox was seleced, the asissged values are shown below.")
        unassigned_df = pd.read_csv(BytesIO(st.session_state.csv_bytes))
        st.dataframe(unassigned_df)