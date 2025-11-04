"""
Fischer Energy Partners - Data Processing Application V4
Raw text preview first, then let user configure parsing
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

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


def read_raw_text(file_path, num_lines=10):
    """Read first N lines as raw text."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [f.readline().rstrip('\n\r') for _ in range(num_lines)]
        return [line for line in lines if line]  # Remove empty lines
    except:
        try:
            with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                lines = [f.readline().rstrip('\n\r') for _ in range(num_lines)]
            return [line for line in lines if line]
        except:
            return None


def parse_with_delimiter(file_path, delimiter, skip_rows, num_rows=8):
    """Parse file with specified delimiter and skip rows."""
    try:
        if str(file_path).endswith('.csv'):
            df = pd.read_csv(
                file_path,
                sep=delimiter,
                header=None,
                skiprows=skip_rows,
                nrows=num_rows,
                engine='python' if delimiter == 'auto' else 'c',
                encoding='utf-8',
                encoding_errors='ignore',
                on_bad_lines='skip'
            )
        else:
            df = pd.read_excel(file_path, header=None, skiprows=skip_rows, nrows=num_rows)

        df.index = [f"Row {skip_rows + i}" for i in range(len(df))]
        return df
    except Exception as e:
        return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Energy Partners - Data Processing Tool")
    st.markdown("**V4: Raw Preview Mode** - See your files exactly as they are, then configure")
    st.markdown("---")

    # Main tabs
    tab1, tab2, tab3 = st.tabs(["1️⃣ Upload & Configure", "2️⃣ Process Data", "3️⃣ Results & Export"])

    # ========== TAB 1: UPLOAD & CONFIGURE ==========
    with tab1:
        st.header("Upload Files and Configure")

        # File uploader
        uploaded_files = st.file_uploader(
            "📁 Upload your sensor files",
            type=['xlsx', 'csv', 'xls'],
            accept_multiple_files=True
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

                    # Initialize config
                    st.session_state.file_configs[uploaded_file.name] = {
                        'skip_rows': 1,  # Default: skip first row (metadata)
                        'delimiter': ',',  # Default: comma
                        'header_row': 0,  # After skipping, which row is header
                        'date_column': 0,
                        'value_column': 2,
                        'excel_time_column': None,
                        'sensor_name': Path(uploaded_file.name).stem
                    }

            st.success(f"✅ {len(uploaded_files)} files uploaded")
            st.markdown("---")

            # Configure each file
            st.subheader("Configure Each File")

            for file_name in st.session_state.uploaded_files.keys():
                file_path = st.session_state.uploaded_files[file_name]
                config = st.session_state.file_configs[file_name]

                with st.expander(f"📄 {file_name}", expanded=True):

                    # === STEP 1: RAW TEXT PREVIEW ===
                    st.markdown("### Step 1: Raw File Preview")
                    st.caption("First 10 lines of the file, exactly as stored:")

                    raw_lines = read_raw_text(file_path, num_lines=10)

                    if raw_lines:
                        # Show raw text in a code block
                        for i, line in enumerate(raw_lines):
                            st.text(f"Line {i}: {line[:150]}{'...' if len(line) > 150 else ''}")
                    else:
                        st.error("Could not read file as text")
                        continue

                    st.markdown("---")

                    # === STEP 2: PARSING CONFIGURATION ===
                    st.markdown("### Step 2: Configure Parsing")

                    parse_col1, parse_col2 = st.columns(2)

                    with parse_col1:
                        config['skip_rows'] = st.number_input(
                            "Skip first N rows",
                            min_value=0,
                            max_value=5,
                            value=config['skip_rows'],
                            key=f"skip_{file_name}",
                            help="How many rows to skip before reading data (e.g., skip row 0 if it has metadata)"
                        )

                    with parse_col2:
                        delimiter_options = {
                            'Comma (,)': ',',
                            'Tab': '\t',
                            'Semicolon (;)': ';',
                            'Pipe (|)': '|',
                            'Auto-detect': None
                        }

                        selected_delim = st.selectbox(
                            "Column delimiter",
                            options=list(delimiter_options.keys()),
                            index=0,
                            key=f"delim_{file_name}",
                            help="What separates the columns in your file?"
                        )
                        config['delimiter'] = delimiter_options[selected_delim]

                    # Parse with current settings
                    parsed_df = parse_with_delimiter(
                        file_path,
                        config['delimiter'],
                        config['skip_rows'],
                        num_rows=8
                    )

                    st.markdown("---")

                    # === STEP 3: PARSED PREVIEW ===
                    st.markdown("### Step 3: Parsed Preview")
                    st.caption("File after applying skip rows and delimiter:")

                    if parsed_df is not None and len(parsed_df) > 0:
                        st.dataframe(parsed_df, use_container_width=True)

                        # Get column info
                        num_cols = len(parsed_df.columns)
                        col_names = [f"Col {i}" for i in range(num_cols)]

                        st.markdown("---")

                        # === STEP 4: COLUMN SELECTION ===
                        st.markdown("### Step 4: Select Columns")

                        cfg_col1, cfg_col2 = st.columns(2)

                        with cfg_col1:
                            st.markdown("##### Data Mapping")

                            # Header row (within the parsed data)
                            config['header_row'] = st.number_input(
                                "Which row has column names?",
                                min_value=0,
                                max_value=min(5, len(parsed_df)-1),
                                value=min(config['header_row'], len(parsed_df)-1),
                                key=f"header_{file_name}",
                                help="Which row (after skipping) contains 'Date', 'Value', etc."
                            )

                            # Show header row values
                            if config['header_row'] < len(parsed_df):
                                header_vals = parsed_df.iloc[config['header_row']].tolist()
                                st.caption(f"Row {config['header_row']} contains: {', '.join([str(v)[:30] for v in header_vals if pd.notna(v)])}")

                            st.markdown("---")

                            # Date column
                            config['date_column'] = st.number_input(
                                "Date column number",
                                min_value=0,
                                max_value=num_cols-1,
                                value=min(config['date_column'], num_cols-1),
                                key=f"date_{file_name}",
                                help="Which column has timestamps?"
                            )

                        with cfg_col2:
                            st.markdown("##### Sensor Info")

                            config['sensor_name'] = st.text_input(
                                "Sensor Name",
                                value=config['sensor_name'],
                                key=f"sensor_{file_name}",
                                help="What to call this sensor in output"
                            )

                            st.markdown("---")

                            # Value column
                            config['value_column'] = st.number_input(
                                "Value column number",
                                min_value=0,
                                max_value=num_cols-1,
                                value=min(config['value_column'], num_cols-1),
                                key=f"value_{file_name}",
                                help="Which column has sensor readings?"
                            )

                            # Excel Time (optional)
                            has_excel_time = st.checkbox(
                                "Has Excel Time column?",
                                value=False,
                                key=f"has_excel_{file_name}"
                            )

                            if has_excel_time:
                                config['excel_time_column'] = st.number_input(
                                    "Excel Time column number",
                                    min_value=0,
                                    max_value=num_cols-1,
                                    value=1,
                                    key=f"excel_{file_name}"
                                )
                            else:
                                config['excel_time_column'] = None

                        # Save config
                        st.session_state.file_configs[file_name] = config

                        # Status
                        st.success(f"✅ Configuration: Skip {config['skip_rows']} rows, use row {config['header_row']} as headers, "
                                 f"column {config['date_column']} = dates, column {config['value_column']} = '{config['sensor_name']}'")

                    else:
                        st.error("❌ Could not parse file with current settings. Try different skip rows or delimiter.")

            # Ready check
            st.markdown("---")
            all_configured = len(st.session_state.file_configs) == len(st.session_state.uploaded_files)

            if all_configured:
                st.success("✅ All files configured! Go to 'Process Data' tab.")
            else:
                st.warning("⚠️ Configure all files first")

    # ========== TAB 2: PROCESS DATA ==========
    with tab2:
        st.header("Process Data")

        col1, col2 = st.columns(2)
        with col1:
            tolerance_minutes = st.slider("Time Tolerance (±minutes)", 1, 5, 2)
        with col2:
            consecutive_repeats = st.slider("Stale Data Threshold", 3, 10, 4)

        if not st.session_state.uploaded_files:
            st.warning("⚠️ Upload files first (Tab 1)")
        else:
            st.info("🚀 Ready to process!")

            with st.expander("📋 Configuration Summary"):
                for file_name, config in st.session_state.file_configs.items():
                    st.text(f"• {file_name} → '{config['sensor_name']}' "
                           f"(Skip {config['skip_rows']}, Header: row {config['header_row']}, "
                           f"Date: col {config['date_column']}, Value: col {config['value_column']})")

            st.markdown("---")

            if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                with st.spinner("Processing..."):

                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    try:
                        # Load files
                        status_text.text("Loading files...")
                        progress_bar.progress(20)

                        loaded_dfs = []
                        for file_name, config in st.session_state.file_configs.items():
                            file_path = st.session_state.uploaded_files[file_name]

                            # Read with configuration
                            if file_path.endswith('.csv'):
                                df_full = pd.read_csv(
                                    file_path,
                                    sep=config['delimiter'],
                                    skiprows=config['skip_rows'],
                                    header=config['header_row'],
                                    engine='python',
                                    encoding='utf-8',
                                    encoding_errors='ignore',
                                    on_bad_lines='skip'
                                )
                            else:
                                df_full = pd.read_excel(
                                    file_path,
                                    skiprows=config['skip_rows'],
                                    header=config['header_row']
                                )

                            # Extract columns by index
                            cols = df_full.columns.tolist()
                            date_col = df_full.iloc[:, config['date_column']]
                            value_col = df_full.iloc[:, config['value_column']]

                            df_clean = pd.DataFrame({
                                'Date': date_col,
                                config['sensor_name']: value_col
                            })

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
