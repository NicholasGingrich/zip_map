import streamlit as st
import boto3
import time
from datetime import datetime

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

generate_button = st.button("Generate Map")

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
                "map_title": map_title or ""
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
if generate_button:
    if not excel_file:
        st.error("Please upload an Excel file.")
        st.stop()
    if not zip_col_label:
        st.error("Please enter a Zip Code column label.")
        st.stop()
    if not value_col_label:
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
        st.write(excel_s3_key)

    # Poll for result
    result_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", ".png")
    progress_text = st.empty()
    progress_bar = st.progress(0)

    poll_interval = 1  
    timeout = 900 
    elapsed = 0

    while elapsed < timeout:
        if check_s3_file_exists(result_s3_key):
            progress_bar.progress(1.0)
            progress_text.text("Done")
            break
        time.sleep(poll_interval)
        elapsed += poll_interval
        progress_bar.progress(min(elapsed / timeout, 0.99))
        progress_text.text(f"Waiting for Map to Finish Generating... {elapsed}s elapsed")

    else:
        st.error("Timed out waiting for Lambda to generate the map.")
        st.stop()

    # Download result and display
    png_bytes = download_s3_file_to_bytes(result_s3_key)

    st.image(png_bytes)

    # Download button
    st.download_button(
        label="Download Map as PNG",
        data=png_bytes,
        file_name="coverage_map.png",
        mime="image/png",
    )

    st.success("Map ready for download")
