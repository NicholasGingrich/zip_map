import streamlit as st
import pandas as pd
import time
from utils import generate_map, fig_to_png_bytes

st.set_page_config(layout="wide")
st.title("ZIP Code Coverage Map")

# -----------------------------
# User inputs
# -----------------------------
zip_col_label = st.text_input(
    label="Zip Code Column Label",
    help="Enter the name of the column in your Excel file that contains ZIP codes.",
)

value_col_label = st.text_input(
    label="Value Column Label",
    help="Enter the name of the column in your Excel file that contains the values to map.",
)

map_title = st.text_input(
    label="Map Title (Optional)",
    help="Enter a title for your map. This is just for display purposes.",
)

color_data_file = st.file_uploader("Upload Excel File", type=["xlsx"])

# -----------------------------
# Advanced options
# -----------------------------
with st.expander("Advanced Options"):
    assign_unassigned_zips = st.checkbox(
        label="Auto-assign Missing Zip Codes",
        value=True,
        help="If selected, will auto assign missing zip codes based on proximity to other zipcodes.",
    )

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

    st.divider()
    st.write(
        "Click on a square to change the color. Colors will be included in the map "
        "from left to right based on the required number of colors to account for all values."
    )

    cols = st.columns(len(map_colors))
    selected_colors = []
    for col, default in zip(cols, map_colors):
        with col:
            selected_colors.append(st.color_picker(" ", default))

# -----------------------------
# Generate map button
# -----------------------------
generate_button = st.button("Generate Map")

if generate_button:
    # -----------------------------
    # Input validation
    # -----------------------------
    if not zip_col_label:
        st.error("Please enter a value for 'Zip Column Label'")
        st.stop()
    if not value_col_label:
        st.error("Please enter a value for 'Value Column Label'")
        st.stop()
    if not color_data_file:
        st.error("Please upload an Excel file.")
        st.stop()

    # -----------------------------
    # Progress placeholder
    # -----------------------------
    progress_bar = st.progress(0.0)
    status_text = st.empty()

    try:
        # Stage 1: Read Excel
        status_text.text("Reading Excel file...")
        data_df = pd.read_excel(color_data_file)
        progress_bar.progress(0.1)
        time.sleep(0.1)  # flush UI

        # Validate columns
        if zip_col_label not in data_df.columns:
            st.error(f"ZIP column '{zip_col_label}' not found in file.")
            st.stop()
        if value_col_label not in data_df.columns:
            st.error(f"Value column '{value_col_label}' not found in file.")
            st.stop()
        progress_bar.progress(0.2)

        # Stage 2: Generate map
        status_text.text("Generating map (may take a while)...")
        fig, unassigned_df = generate_map(
            data_df=data_df,
            auto_fill_unassigned=assign_unassigned_zips,
            zip_col=zip_col_label,
            map_colors=selected_colors,
            value_col=value_col_label,
            map_title=map_title,
        )
        progress_bar.progress(0.8)

        # Stage 3: Convert figure to PNG
        status_text.text("Converting map to PNG for download...")
        fig_bytes = fig_to_png_bytes(fig)
        progress_bar.progress(1.0)

        # -----------------------------
        # Display results
        # -----------------------------
        st.pyplot(fig)

        st.download_button(
            label="Download map as PNG",
            data=fig_bytes,
            file_name="zip_coverage_map.png",
            mime="image/png",
        )

        if unassigned_df is not None and not unassigned_df.empty:
            with st.expander("View Unassigned Zip Codes"):
                st.write(unassigned_df)

        status_text.text("Map generation complete!")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        progress_bar.empty()
        status_text.text("")
