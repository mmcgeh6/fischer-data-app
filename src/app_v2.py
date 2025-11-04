"""
Fischer Energy Partners - Data Processing Application V2
Interactive configuration with user-guided column selection
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Import our data processor
from data_processor import DataProcessor

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = {}
if 'file_configs' not in st.session_state:
    st.session_state.file_configs = {}
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False


def preview_file(file_path, num_rows=10):
    """Read and display first few rows of a file without any processing."""
    try:
        # Read raw file with no header
        df = pd.read_csv(file_path, header=None, nrows=num_rows,
                        sep=None, engine='python', on_bad_lines='skip')
        return df
    except Exception as e:
        return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Energy Partners - Data Processing Tool")
    st.markdown("### Interactive Configuration Mode")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Global Settings")

        # Default settings for "Apply to All"
        st.markdown("### Default Configuration")
        default_header_row = st.number_input(
            "Header Row (0-based index)",
            min_value=0,
            max_value=10,
            value=1,
            help="Which row contains column names?"
        )

        default_value_col = st.text_input(
            "Value Column Name",
            value="Value",
            help="Name of the column containing sensor values"
        )

        default_date_col = st.text_input(
            "Date Column Name",
            value="Date",
            help="Name of the column containing timestamps"
        )

        if st.button("📋 Apply Defaults to All Files", use_container_width=True):
            for file_name in st.session_state.uploaded_files.keys():
                if file_name not in st.session_state.file_configs:
                    st.session_state.file_configs[file_name] = {}
                st.session_state.file_configs[file_name].update({
                    'header_row': default_header_row,
                    'value_column': default_value_col,
                    'date_column': default_date_col,
                    'sensor_name': Path(file_name).stem
                })
            st.success("Applied to all files!")
            st.rerun()

        st.markdown("---")
        st.markdown("### Processing Settings")
        tolerance_minutes = st.slider(
            "Time Tolerance (minutes)",
            min_value=1,
            max_value=5,
            value=2
        )

        consecutive_repeats = st.slider(
            "Stale Data Threshold",
            min_value=3,
            max_value=10,
            value=4
        )

    # Main tabs
    tab1, tab2, tab3 = st.tabs(["1️⃣ Upload & Configure", "2️⃣ Process Data", "3️⃣ Results & Export"])

    # ========== TAB 1: UPLOAD & CONFIGURE ==========
    with tab1:
        st.header("Upload Files and Configure Each Sensor")

        st.info("📁 Upload your sensor files, then configure how each file should be read")

        # File uploader
        uploaded_files = st.file_uploader(
            "Choose files",
            type=['xlsx', 'csv', 'xls'],
            accept_multiple_files=True,
            help="You can select multiple files at once"
        )

        if uploaded_files:
            # Save uploaded files to temp directory
            temp_dir = Path("temp")
            temp_dir.mkdir(exist_ok=True)

            for uploaded_file in uploaded_files:
                file_path = temp_dir / uploaded_file.name

                # Save file if not already saved
                if uploaded_file.name not in st.session_state.uploaded_files:
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    st.session_state.uploaded_files[uploaded_file.name] = str(file_path)

            st.success(f"✅ {len(uploaded_files)} files uploaded")

            # Display configuration for each file
            st.markdown("---")
            st.subheader("Configure Each File")

            for file_name, file_path in st.session_state.uploaded_files.items():
                with st.expander(f"⚙️ Configure: {file_name}", expanded=True):

                    # Create two columns for config and preview
                    col1, col2 = st.columns([1, 2])

                    with col1:
                        st.markdown("#### Configuration")

                        # Initialize config for this file if not exists
                        if file_name not in st.session_state.file_configs:
                            st.session_state.file_configs[file_name] = {
                                'header_row': default_header_row,
                                'value_column': default_value_col,
                                'date_column': default_date_col,
                                'sensor_name': Path(file_name).stem
                            }

                        # Configuration inputs
                        config = st.session_state.file_configs[file_name]

                        config['header_row'] = st.number_input(
                            "Header Row",
                            min_value=0,
                            max_value=10,
                            value=config.get('header_row', default_header_row),
                            key=f"header_{file_name}",
                            help="Row index where column names are (0 = first row)"
                        )

                        config['sensor_name'] = st.text_input(
                            "Sensor Name",
                            value=config.get('sensor_name', Path(file_name).stem),
                            key=f"sensor_{file_name}",
                            help="What to call this sensor in the output"
                        )

                        config['date_column'] = st.text_input(
                            "Date Column",
                            value=config.get('date_column', default_date_col),
                            key=f"date_{file_name}"
                        )

                        config['value_column'] = st.text_input(
                            "Value Column",
                            value=config.get('value_column', default_value_col),
                            key=f"value_{file_name}"
                        )

                        # Update the config
                        st.session_state.file_configs[file_name] = config

                        # Status indicator
                        is_configured = all([
                            config.get('header_row') is not None,
                            config.get('sensor_name'),
                            config.get('date_column'),
                            config.get('value_column')
                        ])

                        if is_configured:
                            st.success("✅ Configured")
                        else:
                            st.warning("⚠️ Incomplete")

                    with col2:
                        st.markdown("#### File Preview (First 10 Rows)")

                        # Show raw preview
                        preview_df = preview_file(file_path, num_rows=10)

                        if preview_df is not None:
                            st.dataframe(preview_df, use_container_width=True)
                        else:
                            st.error("Could not preview file")

            # Ready to process button
            st.markdown("---")
            all_configured = len(st.session_state.file_configs) == len(st.session_state.uploaded_files)

            if all_configured:
                st.success("✅ All files configured! Ready to process.")
                if st.button("➡️ Go to Processing", type="primary", use_container_width=True):
                    st.info("Go to the 'Process Data' tab to continue")
            else:
                st.warning("⚠️ Configure all files before processing")

    # ========== TAB 2: PROCESS DATA ==========
    with tab2:
        st.header("Process Data")

        if not st.session_state.uploaded_files:
            st.warning("⚠️ Please upload files first (Tab 1)")
        elif not st.session_state.file_configs:
            st.warning("⚠️ Please configure your files first (Tab 1)")
        else:
            st.info("🚀 Ready to process your configured data!")

            # Show summary
            st.markdown("### Processing Summary")
            st.write(f"**Files to process:** {len(st.session_state.uploaded_files)}")
            st.write(f"**Configured sensors:** {len(st.session_state.file_configs)}")

            # Display sensor list
            with st.expander("📋 Sensor List"):
                for file_name, config in st.session_state.file_configs.items():
                    st.text(f"• {config['sensor_name']} (from {file_name})")

            st.markdown("---")

            # Process button
            if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                with st.spinner("Processing... This may take a few moments..."):

                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    try:
                        # Step 1: Load files with user configuration
                        status_text.text("Loading files with your configuration...")
                        progress_bar.progress(20)

                        loaded_dataframes = []
                        for file_name, config in st.session_state.file_configs.items():
                            file_path = st.session_state.uploaded_files[file_name]

                            # Read file with user-specified header row
                            if file_path.endswith('.csv'):
                                df = pd.read_csv(
                                    file_path,
                                    header=config['header_row'],
                                    sep=None,
                                    engine='python',
                                    on_bad_lines='skip'
                                )
                            else:
                                df = pd.read_excel(file_path, header=config['header_row'])

                            # Extract the columns user specified
                            if config['date_column'] in df.columns and config['value_column'] in df.columns:
                                df_clean = df[[config['date_column'], config['value_column']]].copy()
                                df_clean.columns = ['Date', config['sensor_name']]

                                # Parse dates
                                df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='mixed', errors='coerce')
                                df_clean = df_clean.dropna(subset=['Date'])

                                loaded_dataframes.append(df_clean)

                        st.session_state.processor.raw_dataframes = loaded_dataframes

                        # Step 2: Combine
                        status_text.text("Combining sensor data...")
                        progress_bar.progress(40)

                        # Custom combine logic since we already have clean dataframes
                        combined = loaded_dataframes[0].copy()
                        for df in loaded_dataframes[1:]:
                            combined = pd.merge(combined, df, on='Date', how='outer')
                        combined = combined.sort_values('Date').reset_index(drop=True)
                        st.session_state.processor.combined_df = combined

                        # Step 3: Save minute data
                        status_text.text("Saving minute-level data...")
                        progress_bar.progress(50)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        minute_data_path = Path("output") / f"minute_data_{timestamp}.csv"
                        st.session_state.processor.save_minute_data_csv(minute_data_path)

                        # Step 4: Resample
                        status_text.text("Resampling to 15-minute intervals...")
                        progress_bar.progress(70)
                        st.session_state.processor.resample_to_15min(tolerance_minutes=tolerance_minutes)

                        # Step 5: Flag stale data
                        status_text.text("Flagging stale data...")
                        progress_bar.progress(85)
                        st.session_state.processor.flag_stale_data(consecutive_repeats=consecutive_repeats)

                        # Complete
                        status_text.text("Processing complete!")
                        progress_bar.progress(100)

                        st.session_state.processing_complete = True
                        st.success("✅ Processing complete! Go to 'Results & Export' tab.")
                        st.balloons()

                    except Exception as e:
                        st.error(f"❌ Error during processing: {str(e)}")
                        st.exception(e)

    # ========== TAB 3: RESULTS & EXPORT ==========
    with tab3:
        st.header("Results & Export")

        if not st.session_state.processing_complete:
            st.warning("⚠️ Please process your data first (Tab 2)")
        else:
            # Get processing summary
            summary = st.session_state.processor.get_processing_summary()

            # Display summary
            st.markdown("### 📈 Processing Summary")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Rows", summary['total_rows'])
            with col2:
                st.metric("Number of Sensors", summary['num_sensors'])
            with col3:
                st.metric("Inexact Matches", summary['inexact_matches'])
            with col4:
                st.metric("Stale Data Points", summary['stale_data_points'])

            st.markdown(f"**Date Range:** {summary['date_range']}")

            # Show sensor list
            with st.expander("📋 Sensors Included"):
                for sensor in summary['sensor_names']:
                    st.text(f"• {sensor}")

            st.markdown("---")

            # Preview the data
            st.markdown("### 👁️ Data Preview")
            if st.session_state.processor.resampled_df is not None:
                st.dataframe(
                    st.session_state.processor.resampled_df.head(20),
                    use_container_width=True
                )

            st.markdown("---")

            # Export section
            st.markdown("### 💾 Export Data")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Clean 15-Minute Data")
                output_filename = st.text_input(
                    "Filename",
                    value=f"clean_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    key="clean_filename"
                )

                if st.button("📥 Download Clean CSV", type="primary", use_container_width=True):
                    output_path = Path("output") / output_filename
                    success = st.session_state.processor.export_to_csv(output_path)

                    if success:
                        with open(output_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Click to Download",
                                data=f,
                                file_name=output_filename,
                                mime='text/csv',
                                use_container_width=True
                            )
                        st.success(f"✅ File ready: {output_filename}")

            with col2:
                st.markdown("#### Minute-Level Data")
                st.info("Saved to 'output' folder (future: SQL data lake)")

                minute_files = list(Path("output").glob("minute_data_*.csv"))
                if minute_files:
                    latest_minute_file = max(minute_files, key=lambda p: p.stat().st_mtime)
                    st.text(f"📁 {latest_minute_file.name}")

                    with open(latest_minute_file, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download Minute Data",
                            data=f,
                            file_name=latest_minute_file.name,
                            mime='text/csv',
                            use_container_width=True
                        )

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Fischer Energy Partners Data Processing Tool V2 | Interactive Configuration Mode"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
