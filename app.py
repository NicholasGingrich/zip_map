from io import BytesIO
import streamlit as st
import boto3
import time
from datetime import datetime
import pandas as pd
import json


# ----------------------------
# Initialize Streamlit State
# ----------------------------
if "processing" not in st.session_state:
    st.session_state.processing = False

if "png_bytes" not in st.session_state:
    st.session_state.png_bytes = None

if "csv_bytes" not in st.session_state:
    st.session_state.csv_bytes = None

if "selected_colors" not in st.session_state:
    st.session_state.selected_colors = []

if "map_error" not in st.session_state:
    st.session_state.map_error = None

def set_processing():
    st.session_state.processing = True

# -----------------------------
# Configuration
# -----------------------------
AWS_REGION = "us-east-1"
S3_BUCKET = "zip-map-bucket"
UPLOAD_PREFIX = "uploads/"
RESULTS_PREFIX = "results/"

s3 = boto3.client(
    "s3",
    region_name=st.secrets["AWS_DEFAULT_REGION"],
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
)
# -----------------------------
# Page Setup
# -----------------------------
st.set_page_config(layout="wide")
st.title("ZIP Code Coverage Map")

# -----------------------------
# User Inputs
# -----------------------------
map_type = st.radio(label="Map Type", options=["By Zipcode", "By State"], index=0)

geog_label = "Zip Code Column Label" if map_type == "By Zipcode" else "State Column Label"
geog_help_label = "ZIP codes" if map_type == "By Zipcode" else "state names or state abbreviations"

geog_col_label = st.text_input(
    geog_label,
    help=f"Name of the column with your geographic values ({geog_help_label})."
)
value_col_label = st.text_input(
    "Value Column Label",
    help="Name of the column whose values will be used for color-coding the map."
)
map_title = st.text_input(
    "Map Title (Optional)",
    help="Optional map title for display purposes."
)
excel_file = st.file_uploader("Upload Excel File", type=["xlsx"], help="Note: Only data from the first sheet will be read.")

with st.expander(label="Advanced Options"):
    auto_assign_zipcodes = st.checkbox(label="Auto-Assign Missing Zip Codes", value=True)
    st.divider()
    map_colors = [
        "#1579b3",
        "#fb9331",
        "#92e091",
        "#ff474a",
        "#5dc0ea",
        "#fbc895",
        "#B07AA1",
        "#FF9DA7",
        "#bfe2f5",
        "#139638",
    ]

    st.write("Colors are assigned to values in the order shown. Only as many colors as needed will be used.")
    cols = st.columns(len(map_colors))

    selected_colors = []
    for col, default in zip(cols, map_colors):
        with col:
            selected_colors.append(st.color_picker(" ", default))
    st.session_state.selected_colors = selected_colors

generate_button = st.button(
    "Generate Map",
    disabled=st.session_state.processing,
    type="primary",
    on_click=set_processing
)

# -----------------------------
# Helpers
# -----------------------------
def upload_excel_to_s3(file, geog_col, value_col, map_title):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = file.name.replace(".xlsx", f"_{timestamp}.xlsx").replace(" ", "_")
    s3_key = f"{UPLOAD_PREFIX}{safe_name}"

    s3.upload_fileobj(
        file,
        S3_BUCKET,
        s3_key,
        ExtraArgs={
            "Metadata": {
                "geog_col": geog_col,
                "value_col": value_col,
                "map_title": map_title or "",
                "auto_assign_zipcodes": str(auto_assign_zipcodes),
                "selected_colors": str(st.session_state.selected_colors),
                "map_type": map_type
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
    st.session_state.processing = True
    if not excel_file:
        st.session_state.processing = False
        st.error("Please upload an Excel file.")
        st.stop()
    if not geog_col_label:
        st.session_state.processing = False
        st.error("Please enter a Zip Code column label.")
        st.stop()
    if not value_col_label:
        st.session_state.processing = False
        st.error("Please enter a Value column label.")
        st.stop()

    with st.spinner("Validating column names..."):
        try:
            preview_df = pd.read_excel(excel_file, nrows=0)
            columns_lower = [c.strip().lower() for c in preview_df.columns]
            validation_errors = []
            if geog_col_label.strip().lower() not in columns_lower:
                validation_errors.append(f"Column '{geog_col_label}' not found in your Excel file. Available columns: {list(preview_df.columns)}")
            if value_col_label.strip().lower() not in columns_lower:
                validation_errors.append(f"Column '{value_col_label}' not found in your Excel file. Available columns: {list(preview_df.columns)}")
            if validation_errors:
                st.session_state.processing = False
                st.session_state.map_error = validation_errors
        except Exception as e:
            st.session_state.processing = False
            st.session_state.map_error = [f"Could not read Excel file: {e}"]

    if st.session_state.map_error:
        for err in st.session_state.map_error:
            st.error(err)
        st.session_state.map_error = None
        st.stop()

    # Reset buffer position after validation read
    excel_file.seek(0)

    # Upload Excel to S3
    with st.spinner("Uploading File For Processing..."):
        excel_s3_key = upload_excel_to_s3(
            excel_file,
            geog_col_label,
            value_col_label,
            map_title,
        )

    # Poll for result
    result_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", ".png")
    unassigned_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", "_unassigned.csv")
    error_s3_key = excel_s3_key.replace(UPLOAD_PREFIX, RESULTS_PREFIX).replace(".xlsx", "_error.json")

    poll_interval = 1
    timeout = 900
    elapsed = 0

    with st.spinner("Generating Map. Do not close or refresh the page..."):
        status_container = st.empty()
        while elapsed < timeout:
            # Update status text
            status_container.text(f"{elapsed}s elapsed")

            if check_s3_file_exists(error_s3_key):
                error_body = json.loads(download_s3_file_to_bytes(error_s3_key).decode("utf-8"))
                status_container.empty()
                st.session_state.processing = False
                st.session_state.map_error = [error_body["error"]]  
                break
                

            # Check if results exist
            if check_s3_file_exists(result_s3_key) and check_s3_file_exists(unassigned_s3_key):
                break

            time.sleep(poll_interval)
            elapsed += poll_interval
        else:
            # Timeout
            st.session_state.processing = False
            status_container.empty()
            st.session_state.map_error = ["Timed out waiting for Lambda to generate the map."]
            st.stop()

    if st.session_state.get("map_error"):
        st.error(st.session_state.map_error)
        st.session_state.map_error = None
        st.stop()

    # Success — clear the status message
    status_container.empty()

    # Download result and display
    png_bytes = download_s3_file_to_bytes(result_s3_key)
    csv_bytes = download_s3_file_to_bytes(unassigned_s3_key)

    st.session_state.png_bytes = png_bytes
    st.session_state.csv_bytes = csv_bytes

    st.success("Map ready for download")
    st.session_state.processing = False
    st.rerun()

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