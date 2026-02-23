import os
import json
import boto3
import logging
import json
import ast
from io import BytesIO, StringIO
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# -----------------------------
# Configuration
# -----------------------------
REFERENCE_FILES = {
    "state_boundaries.parquet": "/tmp/state_boundaries.parquet",
    "zip_code_boundaries.parquet": "/tmp/zip_code_boundaries.parquet",
    "state_abbv_offsets.json": "/tmp/state_abbv_offsets.json",
}

# -----------------------------
# Helper functions
# -----------------------------
def download_reference_files(bucket_name):
    """Download reference parquet/json files to /tmp if not already there"""
    for key, local_path in REFERENCE_FILES.items():
        if not os.path.exists(local_path):
            s3.download_file(bucket_name, f"reference/{key}", local_path)

def download_excel_file(bucket_name, key):
    """Download the uploaded Excel file to /tmp"""
    local_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket_name, key, local_path)
    return local_path

def upload_result_png(bucket_name, original_key, fig_bytes):
    """Upload the generated PNG to results/"""
    result_key = original_key.replace("uploads/", "results/").replace(".xlsx", ".png")
    s3.put_object(Bucket=bucket_name, Key=result_key, Body=fig_bytes, ContentType="image/png")
    return result_key

def delete_s3_object(bucket_name, key):
    """Delete object from S3"""
    s3.delete_object(Bucket=bucket_name, Key=key)

def get_s3_metadata(bucket, key):
    resp = s3.head_object(Bucket=bucket, Key=key)
    return resp.get("Metadata", {})

def upload_unassigned_csv(bucket_name, original_key, df):
    """Upload unassigned ZIP dataframe as CSV"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    result_key = (
        original_key
        .replace("uploads/", "results/")
        .replace(".xlsx", "_unassigned.csv")
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=result_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    return result_key

# -----------------------------
# Lambda Handler
# -----------------------------
def lambda_handler(event, context):
    bucket_name = None
    object_key = None
    try:
        import pandas as pd
        from zip_utils import generate_map, fig_to_png_bytes
        logger.info("Finished importing pandas and zip_utils functions")
        # S3 trigger info
        record = event["Records"][0]["s3"]
        bucket_name = record["bucket"]["name"]
        object_key = unquote_plus(record["object"]["key"])

        logger.info(f"Processing Trigger Event. Record: {record}\nBucket Name: {bucket_name}\nObject Key: {object_key}")

        # Only handle Excel uploads
        if not object_key.endswith(".xlsx"):
            return {"statusCode": 400, "body": "Not an Excel file"}

        # -----------------------------
        # Download reference files if missing
        # -----------------------------
        logger.info("Downloading Reference Files to /tmp")
        download_reference_files(bucket_name)
        logger.info("Finished Dowloading Reference Files")

        # -----------------------------
        # Download Excel
        # -----------------------------
        logger.info("Downloading excel file to /tmp")
        excel_path = download_excel_file(bucket_name, object_key)
        file_size = os.path.getsize(excel_path)
        logger.info(f"Downloaded excel file size: {file_size} bytes")
        if file_size == 0:
            raise ValueError("Downloaded Excel file is empty — possible S3 download issue.")
        logger.info("Finished Downloading excel file")

        # -----------------------------
        # Read Excel
        # -----------------------------
        logger.info(f"Reading excel file '{excel_path}' into dataframe")
        with open(excel_path, "rb") as f:
            excel_bytes = BytesIO(f.read())

        df = pd.read_excel(excel_bytes, engine="openpyxl")

        # -----------------------------
        # Extract column names from first row (or standardize)
        # Adjust this if you want dynamic columns via Streamlit inputs
        # -----------------------------
        metadata = get_s3_metadata(bucket_name, object_key)
        geog_col = metadata.get("geog_col")
        value_col = metadata.get("value_col")
        map_title = metadata.get("map_title") or None
        auto_assign_missing_zip_codes = True if metadata.get("auto_assign_zipcodes") == "True" else False
        selected_map_colors = ast.literal_eval(metadata.get("selected_colors"))
        map_type=metadata.get("map_type")
        geog_type = "zip" if map_type == "By Zipcode" else "state"
        logger.info(f"Metadata received: {metadata}")

        # -----------------------------
        # Generate map
        # -----------------------------
        logger.info("Generating Map from Excel File")
        fig, unassigned_df = generate_map(
            data_df=df,
            geog_col=geog_col,
            value_col=value_col,
            map_colors=selected_map_colors,
            auto_fill_unassigned=auto_assign_missing_zip_codes,
            map_title=map_title,
            geog_type=geog_type
        )
        logger.info("Map Generation Complete")

        # Convert figure to PNG bytes
        logger.info("Converting map to bytes")
        fig_bytes = fig_to_png_bytes(fig).getvalue()
        logger.info("Finished converting map to bytes")

        # Upload result to S3
        logger.info("Uploading PNG/CSV to S3")
        result_key = upload_result_png(bucket_name, object_key, fig_bytes)
        unassigned_key = upload_unassigned_csv(
            bucket_name,
            object_key,
            unassigned_df
        )
        logger.info(f"Uploaded unassigned CSV to {unassigned_key}")
        logger.info(f"Uploaded PNG to {result_key}")


        # Delete the uploaded Excel
        logger.info("Deleting excel file from S3 /uploads folder")
        delete_s3_object(bucket_name, object_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "result_s3_key": result_key,         
                "unassigned_s3_key": unassigned_key
                })
        }

    except Exception as e:
        logger.exception(f"Error running lambda: {e}")

        if bucket_name and object_key:
            error_key = object_key.replace("uploads/", "results/").replace(".xlsx", "_error.json")
            s3.put_object(
                Bucket=bucket_name,
                Key=error_key,
                Body=json.dumps({"error": str(e)}),
                ContentType="application/json"
            )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
