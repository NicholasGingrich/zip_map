import streamlit as st
import pandas as pd
import time
import threading
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

    st.write("Click on a square to change the color. Colors will be included in the map from left to right based on the required number of colors to account for all values.")
    cols = st.columns(len(map_colors))

    selected_colors = []
    for col, default in zip(cols, map_colors):
        with col:
            selected_colors.append(st.color_picker(" ", default))


generate_button = st.button("Generate Map")

# -----------------------------
# Progress placeholders
# -----------------------------
progress_bar = st.empty()

if generate_button:
    with st.status("Initializing...", expanded=True) as status_spinner:
        # -----------------------------
        # Check if file uploaded
        # -----------------------------
        if zip_col_label is None:
            st.error("Please enter a value for 'Zip Column Label'")
            st.stop()

        if zip_col_label is None:
            st.error("Please enter a value for 'Value Column Label'")
            st.stop()

        if color_data_file is None:
            st.error("Please upload an Excel file.")
            st.stop()

        # Immediately show progress bar and status
        progress_bar = st.progress(0.01)

        time.sleep(0)  # force UI flush

        # -----------------------------
        # Shared state
        # -----------------------------
        result = {"fig": None}
        map_done_event = threading.Event()
        conversion_event = threading.Event()
        error_event = threading.Event()
        error_message = {"msg": ""}

        # -----------------------------
        # Worker thread: read Excel, validate, generate map
        # -----------------------------
        def map_worker():
            try:
                # Read Excel
                data_df = pd.read_excel(color_data_file)

                # Validation
                if zip_col_label not in data_df.columns:
                    error_message["msg"] = f"ZIP column '{zip_col_label}' not found."
                    error_event.set()
                    return
                if value_col_label not in data_df.columns:
                    error_message["msg"] = f"Value column '{value_col_label}' not found."
                    error_event.set()
                    return

                # Generate map
                fig, unassigned_df = generate_map(
                    data_df=data_df,
                    auto_fill_unassigned=assign_unassigned_zips,
                    zip_col=zip_col_label,
                    map_colors=selected_colors,
                    value_col=value_col_label,
                    map_title=map_title,
                )
                result["fig"] = fig
                result["unassigned_df"] = unassigned_df
                map_done_event.set()

                # Phase 2 - Conversion
                fig_bytes = fig_to_png_bytes(fig)
                result["fig_bytes"] = fig_bytes
                conversion_event.set()

            except Exception as e:
                error_message["msg"] = str(e)
                error_event.set()

        worker_thread = threading.Thread(target=map_worker, daemon=True)
        worker_thread.start()

        # -----------------------------
        # Stage 1: Generating Map (118s max)
        # -----------------------------
        stage1_duration = 420
        stage1_start = time.time()

        status_spinner.update(label="Generating Map, this may take a while...")
        while not map_done_event.is_set() and not error_event.is_set():
            elapsed = time.time() - stage1_start
            fraction = min(elapsed / stage1_duration, 0.72)
            progress_bar.progress(fraction)
            time.sleep(0.5)

        # Ensure phase 1 visually completes
        progress_bar.progress(0.72)

        # -----------------------------
        # Stage 2: Converting Map
        # -----------------------------
        stage2_duration = 124
        stage2_start = time.time()
        status_spinner.update(label="Converting Map to Downloadable Form")
        while not error_event.is_set() and not conversion_event.is_set():
            elapsed = time.time() - stage2_start
            fraction = 0.72 + (elapsed / stage2_duration) * 0.05
            progress_bar.progress(min(fraction, 0.99))
            time.sleep(0.3)

        # -----------------------------
        # Finalize
        # -----------------------------
        @st.fragment()
        def plot_graph():
            fig_placeholder = st.empty()

            if error_event.is_set():
                st.error(error_message["msg"])
            else:
                fig = result["fig"]

                if fig is not None:
                    fig_placeholder.pyplot(fig)
                    progress_bar.progress(1.0)
                    status_spinner.update(label="Map Generation Complete")
                    progress_bar.empty()
                else:
                    st.error("Map generation failed.")

        @st.fragment()
        def show_download_button():
            fig_bytes = result["fig_bytes"]
            download_placeholder = st.empty()

            if fig_bytes is not None:
                download_placeholder.download_button(
                    label="Download map as PNG",
                    data=fig_bytes,
                    file_name="zip_coverage_map.png",
                    mime="image/png",
                )
            else:
                st.error("Map generation failed.")

        @st.fragment()
        def show_unassigned_df():
            unassigned_df = result["unassigned_df"]
            if unassigned_df is not None:
                # Add in pannel with unassigned info
                st.write(unassigned_df)
            else:
                st.error("Map generation failed.")

        plot_graph()
        show_download_button()
        show_unassigned_df()
