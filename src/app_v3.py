"""
Fischer Energy Partners - Data Processing Application V3
Simplified interactive configuration
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

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


def read_first_rows(file_path, num_rows=8):
    """Read first N rows of a file with no processing. Uses multiple fallback strategies."""

    if str(file_path).endswith('.csv'):
        # Try multiple strategies for CSV files
        strategies = [
            # Strategy 1: Python engine with flexible separator
            {
                'sep': None,
                'engine': 'python',
                'encoding': 'utf-8',
                'on_bad_lines': 'skip',
                'quoting': 3  # QUOTE_NONE
            },
            # Strategy 2: Explicit comma separator
            {
                'sep': ',',
                'engine': 'python',
                'encoding': 'utf-8',
                'on_bad_lines': 'skip'
            },
            # Strategy 3: Different encoding
            {
                'sep': None,
                'engine': 'python',
                'encoding': 'latin-1',
                'on_bad_lines': 'skip'
            },
            # Strategy 4: C engine (faster but less flexible)
            {
                'sep': ',',
                'encoding': 'utf-8',
                'on_bad_lines': 'skip'
            }
        ]

        for strategy in strategies:
            try:
                df = pd.read_csv(
                    file_path,
                    header=None,
                    nrows=num_rows,
                    encoding_errors='ignore',
                    **strategy
                )

                # If we got rows, use this strategy
                if len(df) > 0:
                    df.index = [f"Row {i}" for i in range(len(df))]
                    return df
            except Exception:
                continue

        # If all strategies failed, return None
        return None

    else:
        # Excel files
        try:
            df = pd.read_excel(file_path, header=None, nrows=num_rows)
            df.index = [f"Row {i}" for i in range(len(df))]
            return df
        except Exception as e:
            st.error(f"Error reading Excel file: {e}")
            return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Energy Partners - Data Processing Tool")
    st.markdown("---")

    # Main tabs
    tab1, tab2, tab3 = st.tabs(["1️⃣ Upload & Configure", "2️⃣ Process Data", "3️⃣ Results & Export"])

    # ========== TAB 1: UPLOAD & CONFIGURE ==========
    with tab1:
        st.header("Upload Files and Configure")

        st.info("📁 Upload your sensor files, then tell us which row/columns to use")

        # File uploader
        uploaded_files = st.file_uploader(
            "Choose files",
            type=['xlsx', 'csv', 'xls'],
            accept_multiple_files=True,
            help="Select all your sensor data files"
        )

        if uploaded_files:
            # Save uploaded files
            temp_dir = Path("temp")
            temp_dir.mkdir(exist_ok=True)

            for uploaded_file in uploaded_files:
                file_path = temp_dir / uploaded_file.name

                if uploaded_file.name not in st.session_state.uploaded_files:
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    st.session_state.uploaded_files[uploaded_file.name] = str(file_path)

                    # Initialize config with defaults
                    st.session_state.file_configs[uploaded_file.name] = {
                        'header_row': None,
                        'date_column': None,
                        'value_column': None,
                        'excel_time_column': None,
                        'sensor_name': Path(uploaded_file.name).stem
                    }

            st.success(f"✅ {len(uploaded_files)} files uploaded")
            st.markdown("---")

            # Configure each file
            st.subheader("Configure Each File")
            st.markdown("*For each file, select which row contains headers and which columns contain your data*")

            for file_name in st.session_state.uploaded_files.keys():
                file_path = st.session_state.uploaded_files[file_name]
                config = st.session_state.file_configs[file_name]

                with st.expander(f"📄 {file_name}", expanded=True):

                    # Show file preview
                    st.markdown("**First 8 Rows of File:**")
                    preview_df = read_first_rows(file_path, num_rows=8)

                    if preview_df is not None:
                        st.dataframe(preview_df, use_container_width=True)

                        # Get column names as they appear in the preview
                        col_options = [f"Column {i}" for i in range(len(preview_df.columns))]
                        row_options = list(range(len(preview_df)))

                        st.markdown("---")

                        # Configuration in columns
                        cfg_col1, cfg_col2 = st.columns(2)

                        with cfg_col1:
                            st.markdown("##### Row & Column Selection")

                            # Header row selection
                            # Default to row 1 if available, otherwise row 0
                            default_header_idx = min(1, len(row_options) - 1)
                            if config['header_row'] is not None and config['header_row'] < len(row_options):
                                default_header_idx = config['header_row']

                            config['header_row'] = st.selectbox(
                                "Which row contains column names?",
                                options=row_options,
                                index=default_header_idx,
                                key=f"header_row_{file_name}",
                                help="Select the row number that has 'Date', 'Value', etc."
                            )

                            # Show what's in that header row
                            if config['header_row'] is not None:
                                header_values = preview_df.iloc[config['header_row']].tolist()
                                st.caption(f"Values in row {config['header_row']}: {', '.join([str(v) for v in header_values if pd.notna(v)])}")

                            st.markdown("---")

                            # Date column - show actual column values if header row is selected
                            if config['header_row'] is not None:
                                header_row = preview_df.iloc[config['header_row']]
                                column_labels = {i: f"{i}: {str(header_row[i])}" for i in range(len(header_row)) if pd.notna(header_row[i])}
                            else:
                                column_labels = {i: f"Column {i}" for i in range(len(preview_df.columns))}

                            selected_date_idx = st.selectbox(
                                "Which column has dates/timestamps?",
                                options=list(column_labels.keys()),
                                format_func=lambda x: column_labels[x],
                                index=0,
                                key=f"date_col_{file_name}"
                            )
                            config['date_column'] = selected_date_idx

                        with cfg_col2:
                            st.markdown("##### Sensor Configuration")

                            # Sensor name
                            config['sensor_name'] = st.text_input(
                                "Sensor Name (for output)",
                                value=config['sensor_name'],
                                key=f"sensor_name_{file_name}",
                                help="This will be the column name in the final output"
                            )

                            st.markdown("---")

                            # Value column
                            selected_value_idx = st.selectbox(
                                "Which column has sensor values?",
                                options=list(column_labels.keys()),
                                format_func=lambda x: column_labels[x],
                                index=min(2, len(column_labels)-1),
                                key=f"value_col_{file_name}"
                            )
                            config['value_column'] = selected_value_idx

                            # Excel Time column (optional)
                            excel_time_options = [-1] + list(column_labels.keys())
                            selected_excel_time = st.selectbox(
                                "Excel Time column (optional)",
                                options=excel_time_options,
                                format_func=lambda x: "None" if x == -1 else column_labels[x],
                                index=0,
                                key=f"excel_time_col_{file_name}"
                            )
                            config['excel_time_column'] = None if selected_excel_time == -1 else selected_excel_time

                        # Save config
                        st.session_state.file_configs[file_name] = config

                        # Status
                        is_configured = (
                            config['header_row'] is not None and
                            config['date_column'] is not None and
                            config['value_column'] is not None and
                            config['sensor_name']
                        )

                        if is_configured:
                            st.success(f"✅ Configured: Will read row {config['header_row']} as headers, "
                                     f"column {config['date_column']} as dates, "
                                     f"column {config['value_column']} as '{config['sensor_name']}'")
                        else:
                            st.warning("⚠️ Please complete all selections")

                    else:
                        st.error("Could not read file preview")

            # Ready check
            st.markdown("---")
            all_configured = all(
                c.get('header_row') is not None and
                c.get('date_column') is not None and
                c.get('value_column') is not None
                for c in st.session_state.file_configs.values()
            )

            if all_configured:
                st.success("✅ All files configured! Ready to process.")
            else:
                st.warning("⚠️ Please configure all files before processing")

    # ========== TAB 2: PROCESS DATA ==========
    with tab2:
        st.header("Process Data")

        # Processing settings
        col1, col2 = st.columns(2)
        with col1:
            tolerance_minutes = st.slider("Time Tolerance (±minutes)", 1, 5, 2)
        with col2:
            consecutive_repeats = st.slider("Stale Data Threshold", 3, 10, 4)

        if not st.session_state.uploaded_files:
            st.warning("⚠️ Please upload files first (Tab 1)")
        else:
            all_configured = all(
                c.get('header_row') is not None and
                c.get('date_column') is not None and
                c.get('value_column') is not None
                for c in st.session_state.file_configs.values()
            )

            if not all_configured:
                st.warning("⚠️ Please configure all files first (Tab 1)")
            else:
                st.info("🚀 Ready to process!")

                # Show summary
                with st.expander("📋 Processing Configuration Summary"):
                    for file_name, config in st.session_state.file_configs.items():
                        st.text(f"• {file_name} → '{config['sensor_name']}' "
                               f"(Header: row {config['header_row']}, "
                               f"Date: col {config['date_column']}, "
                               f"Value: col {config['value_column']})")

                st.markdown("---")

                # Process button
                if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                    with st.spinner("Processing..."):

                        progress_bar = st.progress(0)
                        status_text = st.empty()

                        try:
                            # Load files with user configuration
                            status_text.text("Loading files...")
                            progress_bar.progress(20)

                            loaded_dfs = []
                            for file_name, config in st.session_state.file_configs.items():
                                file_path = st.session_state.uploaded_files[file_name]

                                # Read file
                                if file_path.endswith('.csv'):
                                    df_full = pd.read_csv(file_path, header=config['header_row'],
                                                         sep=None, engine='python', on_bad_lines='skip')
                                else:
                                    df_full = pd.read_excel(file_path, header=config['header_row'])

                                # Get the actual column names after reading with header
                                cols = df_full.columns.tolist()
                                date_col_name = cols[config['date_column']]
                                value_col_name = cols[config['value_column']]

                                # Extract just what we need
                                df_clean = df_full[[date_col_name, value_col_name]].copy()
                                df_clean.columns = ['Date', config['sensor_name']]

                                # Parse dates
                                df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='mixed', errors='coerce')
                                df_clean = df_clean.dropna(subset=['Date'])

                                loaded_dfs.append(df_clean)

                            # Combine
                            status_text.text("Combining sensors...")
                            progress_bar.progress(40)

                            combined = loaded_dfs[0].copy()
                            for df in loaded_dfs[1:]:
                                combined = pd.merge(combined, df, on='Date', how='outer')
                            combined = combined.sort_values('Date').reset_index(drop=True)
                            st.session_state.processor.combined_df = combined

                            # Save minute data
                            status_text.text("Saving minute-level data...")
                            progress_bar.progress(50)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            minute_path = Path("output") / f"minute_data_{timestamp}.csv"
                            st.session_state.processor.save_minute_data_csv(minute_path)

                            # Resample
                            status_text.text("Resampling to 15-minute intervals...")
                            progress_bar.progress(70)
                            st.session_state.processor.resample_to_15min(tolerance_minutes=tolerance_minutes)

                            # Flag stale
                            status_text.text("Flagging stale data...")
                            progress_bar.progress(85)
                            st.session_state.processor.flag_stale_data(consecutive_repeats=consecutive_repeats)

                            # Complete
                            status_text.text("Complete!")
                            progress_bar.progress(100)

                            st.session_state.processing_complete = True
                            st.success("✅ Processing complete! Go to 'Results & Export'")
                            st.balloons()

                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                            st.exception(e)

    # ========== TAB 3: RESULTS & EXPORT ==========
    with tab3:
        st.header("Results & Export")

        if not st.session_state.processing_complete:
            st.warning("⚠️ Process your data first (Tab 2)")
        else:
            summary = st.session_state.processor.get_processing_summary()

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", summary['total_rows'])
            with col2:
                st.metric("Sensors", summary['num_sensors'])
            with col3:
                st.metric("Inexact Matches", summary['inexact_matches'])
            with col4:
                st.metric("Stale Points", summary['stale_data_points'])

            st.markdown(f"**Date Range:** {summary['date_range']}")

            st.markdown("---")
            st.markdown("### 👁️ Data Preview")
            st.dataframe(st.session_state.processor.resampled_df.head(20), use_container_width=True)

            st.markdown("---")
            st.markdown("### 💾 Export")

            col1, col2 = st.columns(2)

            with col1:
                output_filename = st.text_input(
                    "Filename",
                    value=f"clean_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )

                if st.button("📥 Generate Clean CSV", type="primary", use_container_width=True):
                    output_path = Path("output") / output_filename
                    if st.session_state.processor.export_to_csv(output_path):
                        with open(output_path, 'rb') as f:
                            st.download_button(
                                "⬇️ Download",
                                data=f,
                                file_name=output_filename,
                                mime='text/csv',
                                use_container_width=True
                            )
                        st.success("✅ Ready!")

            with col2:
                st.info("Minute-level data saved to 'output' folder")
                minute_files = list(Path("output").glob("minute_data_*.csv"))
                if minute_files:
                    latest = max(minute_files, key=lambda p: p.stat().st_mtime)
                    with open(latest, 'rb') as f:
                        st.download_button(
                            "⬇️ Download Minute Data",
                            data=f,
                            file_name=latest.name,
                            mime='text/csv',
                            use_container_width=True
                        )


if __name__ == "__main__":
    main()
