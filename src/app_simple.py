"""
Fischer Energy Partners - Data Processing Application (Simple Version)
Load → Configure → Combine → Export
No resampling, no flags - just combine the data as-is
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing - Simple",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = {}
if 'file_configs' not in st.session_state:
    st.session_state.file_configs = {}
if 'combined_df' not in st.session_state:
    st.session_state.combined_df = None


def read_raw_text(file_path, num_lines=10):
    """Read first N lines as raw text."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [f.readline().rstrip('\n\r') for _ in range(num_lines)]
        return [line for line in lines if line]
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
                engine='python' if delimiter is None else 'c',
                encoding='utf-8',
                encoding_errors='ignore',
                on_bad_lines='skip'
            )
        else:
            df = pd.read_excel(file_path, header=None, skiprows=skip_rows, nrows=num_rows)

        df.index = [f"Row {skip_rows + i}" for i in range(len(df))]
        return df
    except Exception as e:
        st.error(f"Parse error: {e}")
        return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Data Processing - Simple Version")
    st.markdown("**Load → Configure → Combine → Export** (no resampling)")
    st.markdown("---")

    # Main tabs
    tab1, tab2, tab3 = st.tabs(["1️⃣ Upload & Configure", "2️⃣ Combine Data", "3️⃣ Export"])

    # ========== TAB 1: UPLOAD & CONFIGURE ==========
    with tab1:
        st.header("Upload and Configure Files")

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

                    # Initialize default config
                    st.session_state.file_configs[uploaded_file.name] = {
                        'skip_rows': 1,
                        'delimiter': ',',
                        'header_row': 0,
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

                    # === STEP 1: RAW PREVIEW ===
                    st.markdown("### Step 1: Raw File Preview")
                    raw_lines = read_raw_text(file_path, num_lines=10)

                    if raw_lines:
                        for i, line in enumerate(raw_lines):
                            st.text(f"Line {i}: {line[:120]}{'...' if len(line) > 120 else ''}")
                    else:
                        st.error("Could not read file")
                        continue

                    st.markdown("---")

                    # === STEP 2: PARSING CONFIG ===
                    st.markdown("### Step 2: Configure Parsing")

                    parse_col1, parse_col2 = st.columns(2)

                    with parse_col1:
                        config['skip_rows'] = st.number_input(
                            "Skip first N rows",
                            min_value=0,
                            max_value=5,
                            value=config['skip_rows'],
                            key=f"skip_{file_name}",
                            help="Skip metadata rows at the top"
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
                            key=f"delim_{file_name}"
                        )
                        config['delimiter'] = delimiter_options[selected_delim]

                    # Parse preview
                    parsed_df = parse_with_delimiter(
                        file_path,
                        config['delimiter'],
                        config['skip_rows'],
                        num_rows=8
                    )

                    st.markdown("---")

                    # === STEP 3: PARSED PREVIEW ===
                    st.markdown("### Step 3: Parsed Preview")

                    if parsed_df is not None and len(parsed_df) > 0:
                        st.dataframe(parsed_df, use_container_width=True)

                        num_cols = len(parsed_df.columns)

                        st.markdown("---")

                        # === STEP 4: COLUMN SELECTION ===
                        st.markdown("### Step 4: Select Columns")

                        cfg_col1, cfg_col2 = st.columns(2)

                        with cfg_col1:
                            # Header row
                            config['header_row'] = st.number_input(
                                "Which row has column names?",
                                min_value=0,
                                max_value=min(5, len(parsed_df)-1),
                                value=min(config['header_row'], len(parsed_df)-1),
                                key=f"header_{file_name}"
                            )

                            # Show header values
                            if config['header_row'] < len(parsed_df):
                                header_vals = parsed_df.iloc[config['header_row']].tolist()
                                st.caption(f"→ {', '.join([str(v)[:30] for v in header_vals if pd.notna(v)])}")

                            # Date column
                            config['date_column'] = st.number_input(
                                "Date column number",
                                min_value=0,
                                max_value=num_cols-1,
                                value=min(config['date_column'], num_cols-1),
                                key=f"date_{file_name}"
                            )

                        with cfg_col2:
                            # Sensor name
                            config['sensor_name'] = st.text_input(
                                "Sensor Name (for output)",
                                value=config['sensor_name'],
                                key=f"sensor_{file_name}"
                            )

                            # Value column
                            config['value_column'] = st.number_input(
                                "Value column number",
                                min_value=0,
                                max_value=num_cols-1,
                                value=min(config['value_column'], num_cols-1),
                                key=f"value_{file_name}"
                            )

                            # Excel Time column (optional)
                            has_excel_time = st.checkbox(
                                "Include Excel Time column?",
                                value=False,
                                key=f"has_excel_{file_name}",
                                help="Check if this file has an Excel Time column you want to keep"
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

                        st.success(f"✅ Will combine: Column {config['date_column']} (dates) + Column {config['value_column']} ('{config['sensor_name']}')")

                    else:
                        st.error("❌ Could not parse file. Try different settings.")

            # Ready check
            st.markdown("---")
            if len(st.session_state.file_configs) == len(st.session_state.uploaded_files):
                st.success("✅ All files configured! Go to 'Combine Data' tab.")

    # ========== TAB 2: COMBINE DATA ==========
    with tab2:
        st.header("Combine Data")

        if not st.session_state.uploaded_files:
            st.warning("⚠️ Upload files first (Tab 1)")
        else:
            st.info("🚀 Ready to combine all sensor data!")

            # Show summary
            with st.expander("📋 Files to Combine"):
                for file_name, config in st.session_state.file_configs.items():
                    st.text(f"• {file_name} → '{config['sensor_name']}' "
                           f"(Skip {config['skip_rows']}, Date: col {config['date_column']}, "
                           f"Value: col {config['value_column']})")

            st.markdown("---")

            if st.button("🔗 Combine All Files", type="primary", use_container_width=True):
                with st.spinner("Combining files..."):

                    try:
                        loaded_dfs = []

                        # Load each file with its configuration
                        for file_name, config in st.session_state.file_configs.items():
                            file_path = st.session_state.uploaded_files[file_name]

                            st.text(f"Loading {file_name}...")

                            # Read file
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

                            # Extract just the columns we need by index
                            date_col = df_full.iloc[:, config['date_column']]
                            value_col = df_full.iloc[:, config['value_column']]

                            # Create clean dataframe
                            df_clean = pd.DataFrame({
                                'Date': date_col,
                                config['sensor_name']: value_col
                            })

                            # Include Excel Time if specified
                            if config.get('excel_time_column') is not None:
                                excel_time_col = df_full.iloc[:, config['excel_time_column']]
                                df_clean['Excel Time'] = excel_time_col

                            # Parse dates - keep full precision including milliseconds
                            df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='mixed', errors='coerce')

                            # Remove rows where date couldn't be parsed
                            before = len(df_clean)
                            df_clean = df_clean.dropna(subset=['Date'])
                            after = len(df_clean)

                            if before > after:
                                st.warning(f"⚠️ {file_name}: Removed {before - after} rows with invalid dates")

                            loaded_dfs.append(df_clean)
                            st.success(f"✅ {file_name}: {len(df_clean)} rows loaded")

                        # Combine all dataframes
                        st.text("Merging all sensors by timestamp...")

                        combined = loaded_dfs[0].copy()
                        for df in loaded_dfs[1:]:
                            # Merge on Date, and Excel Time if both have it
                            merge_cols = ['Date']
                            if 'Excel Time' in combined.columns and 'Excel Time' in df.columns:
                                # Merge on both Date and Excel Time
                                combined = pd.merge(combined, df, on=merge_cols, how='outer', suffixes=('', '_dup'))
                                # Consolidate Excel Time columns
                                if 'Excel Time_dup' in combined.columns:
                                    combined['Excel Time'] = combined['Excel Time'].fillna(combined['Excel Time_dup'])
                                    combined = combined.drop(columns=['Excel Time_dup'])
                            else:
                                combined = pd.merge(combined, df, on=merge_cols, how='outer')

                        # Sort by date
                        combined = combined.sort_values('Date').reset_index(drop=True)

                        # Reorder columns: Date, Excel Time (if exists), then sensors
                        cols = ['Date']
                        if 'Excel Time' in combined.columns:
                            cols.append('Excel Time')
                        sensor_cols = [c for c in combined.columns if c not in ['Date', 'Excel Time']]
                        combined = combined[cols + sensor_cols]

                        # Remove timezone info for cleaner export
                        if pd.api.types.is_datetime64tz_dtype(combined['Date']):
                            combined['Date'] = combined['Date'].dt.tz_localize(None)

                        st.session_state.combined_df = combined

                        st.success(f"✅ Combined successfully!")
                        st.balloons()

                        # Show summary
                        st.markdown("### 📊 Combined Data Summary")
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric("Total Rows", len(combined))
                        with col2:
                            st.metric("Sensors", len(combined.columns) - 1)  # Exclude Date column
                        with col3:
                            date_range = f"{combined['Date'].min()} to {combined['Date'].max()}"
                            st.metric("Date Range", "See below")
                            st.caption(date_range)

                        st.markdown("---")
                        st.markdown("### 👁️ Preview (First 20 Rows)")
                        st.dataframe(combined.head(20), use_container_width=True)

                        st.markdown("---")
                        st.info("✅ Data ready! Go to 'Export' tab to download.")

                    except Exception as e:
                        st.error(f"❌ Error combining files: {str(e)}")
                        st.exception(e)

            # Show existing combined data if available
            if st.session_state.combined_df is not None:
                st.markdown("---")
                st.markdown("### Current Combined Data")
                st.dataframe(st.session_state.combined_df.head(10), use_container_width=True)

    # ========== TAB 3: EXPORT ==========
    with tab3:
        st.header("Export Combined Data")

        if st.session_state.combined_df is None:
            st.warning("⚠️ Combine your data first (Tab 2)")
        else:
            combined = st.session_state.combined_df

            # Summary
            st.markdown("### 📊 Data Summary")
            col1, col2 = st.columns(2)

            with col1:
                st.metric("Total Rows", len(combined))
                st.metric("Total Columns", len(combined.columns))

            with col2:
                sensors = [col for col in combined.columns if col != 'Date']
                st.metric("Sensors", len(sensors))
                with st.expander("View sensor list"):
                    for sensor in sensors:
                        st.text(f"• {sensor}")

            st.markdown("---")

            # Preview
            st.markdown("### 👁️ Full Data Preview")
            st.dataframe(combined, use_container_width=True)

            st.markdown("---")

            # Export
            st.markdown("### 💾 Download")

            output_filename = st.text_input(
                "Output filename",
                value=f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )

            if st.button("📥 Generate CSV", type="primary", use_container_width=True):
                try:
                    # Save to output folder
                    output_dir = Path("output")
                    output_dir.mkdir(exist_ok=True)
                    output_path = output_dir / output_filename

                    # Format Date column before export
                    combined_export = combined.copy()
                    if 'Date' in combined_export.columns:
                        # Format as MM/DD/YYYY HH:MM:SS
                        combined_export['Date'] = combined_export['Date'].dt.strftime('%m/%d/%Y %H:%M:%S')

                    combined_export.to_csv(output_path, index=False)

                    st.success(f"✅ Saved to: {output_path}")

                    # Download button
                    with open(output_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Click to Download",
                            data=f,
                            file_name=output_filename,
                            mime='text/csv',
                            use_container_width=True
                        )

                except Exception as e:
                    st.error(f"❌ Error exporting: {str(e)}")

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Fischer Energy Partners - Simple Combine Tool"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
