"""
Fischer Energy Partners - Data Processing Application (Streamlined)
Quick configuration mode for processing 10-90 files efficiently
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing - Streamlined",
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
if 'global_config' not in st.session_state:
    st.session_state.global_config = {
        'skip_rows': 1,
        'delimiter': ',',
        'header_row': 0,
        'date_column': 0,
        'value_column': 2,
        'include_excel_time': False,
        'excel_time_column': 1
    }


def read_raw_text(file_path, num_lines=3):
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


def parse_with_delimiter(file_path, delimiter, skip_rows, num_rows=5):
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
        return df
    except:
        return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Data Processing - Streamlined")
    st.markdown("**Quick Mode**: Configure all files at once, then combine")
    st.markdown("---")

    # ========== STEP 1: UPLOAD ==========
    st.header("Step 1: Upload Files")

    uploaded_files = st.file_uploader(
        "📁 Upload all your sensor files at once",
        type=['xlsx', 'csv', 'xls'],
        accept_multiple_files=True,
        help="Select all files (10-90 files) - you can configure them all together"
    )

    if uploaded_files:
        # Save files
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)

        for uploaded_file in uploaded_files:
            file_path = temp_dir / uploaded_file.name
            if uploaded_file.name not in st.session_state.uploaded_files:
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                st.session_state.uploaded_files[uploaded_file.name] = str(file_path)

        st.success(f"✅ {len(uploaded_files)} files uploaded")
        st.markdown("---")

        # ========== STEP 2: GLOBAL CONFIGURATION ==========
        st.header("Step 2: Configure Parsing (Applies to All Files)")

        with st.form("global_config_form"):
            st.markdown("**Set defaults that will apply to all files**")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                skip_rows = st.number_input(
                    "Skip first N rows",
                    min_value=0,
                    max_value=5,
                    value=st.session_state.global_config['skip_rows'],
                    help="Skip metadata rows"
                )

            with col2:
                delimiter_options = {
                    'Comma (,)': ',',
                    'Tab': '\t',
                    'Semicolon (;)': ';',
                    'Auto-detect': None
                }
                selected_delim = st.selectbox(
                    "Delimiter",
                    options=list(delimiter_options.keys()),
                    index=0
                )
                delimiter = delimiter_options[selected_delim]

            with col3:
                header_row = st.number_input(
                    "Header row",
                    min_value=0,
                    max_value=5,
                    value=st.session_state.global_config['header_row'],
                    help="Which row has column names"
                )

            with col4:
                date_column = st.number_input(
                    "Date column #",
                    min_value=0,
                    max_value=10,
                    value=st.session_state.global_config['date_column']
                )

            col5, col6, col7 = st.columns(3)

            with col5:
                value_column = st.number_input(
                    "Value column #",
                    min_value=0,
                    max_value=10,
                    value=st.session_state.global_config['value_column']
                )

            with col6:
                include_excel_time = st.checkbox(
                    "Include Excel Time?",
                    value=st.session_state.global_config['include_excel_time']
                )

            with col7:
                if include_excel_time:
                    excel_time_column = st.number_input(
                        "Excel Time column #",
                        min_value=0,
                        max_value=10,
                        value=st.session_state.global_config['excel_time_column']
                    )
                else:
                    excel_time_column = None

            st.markdown("---")

            # Apply button
            apply_clicked = st.form_submit_button("✅ Apply to All Files", type="primary", use_container_width=True)

            if apply_clicked:
                # Save global config
                st.session_state.global_config = {
                    'skip_rows': skip_rows,
                    'delimiter': delimiter,
                    'header_row': header_row,
                    'date_column': date_column,
                    'value_column': value_column,
                    'include_excel_time': include_excel_time,
                    'excel_time_column': excel_time_column
                }

                # Apply to all files
                for file_name in st.session_state.uploaded_files.keys():
                    st.session_state.file_configs[file_name] = {
                        'skip_rows': skip_rows,
                        'delimiter': delimiter,
                        'header_row': header_row,
                        'date_column': date_column,
                        'value_column': value_column,
                        'excel_time_column': excel_time_column if include_excel_time else None,
                        'sensor_name': Path(file_name).stem
                    }

                st.success(f"✅ Configuration applied to all {len(st.session_state.uploaded_files)} files!")
                st.rerun()

        # ========== STEP 3: PREVIEW & REVIEW ==========
        if st.session_state.file_configs:
            st.markdown("---")
            st.header("Step 3: Review Configuration")

            # Show sample preview from first file
            st.markdown("### Sample Preview (First File)")
            first_file = list(st.session_state.uploaded_files.keys())[0]
            first_path = st.session_state.uploaded_files[first_file]
            first_config = st.session_state.file_configs[first_file]

            col_prev1, col_prev2 = st.columns(2)

            with col_prev1:
                st.markdown("**Raw text (first 3 lines):**")
                raw_lines = read_raw_text(first_path, num_lines=3)
                if raw_lines:
                    for i, line in enumerate(raw_lines):
                        st.text(f"{i}: {line[:80]}...")

            with col_prev2:
                st.markdown("**Parsed preview:**")
                parsed = parse_with_delimiter(
                    first_path,
                    first_config['delimiter'],
                    first_config['skip_rows'],
                    num_rows=5
                )
                if parsed is not None:
                    st.dataframe(parsed, use_container_width=True)

            st.markdown("---")

            # Files summary table
            st.markdown("### All Files Summary")

            summary_data = []
            for file_name, config in st.session_state.file_configs.items():
                summary_data.append({
                    'File': file_name,
                    'Sensor Name': config['sensor_name'],
                    'Skip': config['skip_rows'],
                    'Header Row': config['header_row'],
                    'Date Col': config['date_column'],
                    'Value Col': config['value_column']
                })

            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True, height=min(400, len(summary_df) * 35 + 38))

            # Option to edit individual files
            with st.expander("⚙️ Need to adjust individual files? (Optional)"):
                st.info("Most files should work with the global settings. Only expand this if you need to customize specific files.")

                edit_file = st.selectbox(
                    "Select file to customize",
                    options=list(st.session_state.file_configs.keys())
                )

                if edit_file:
                    config = st.session_state.file_configs[edit_file]

                    st.markdown(f"**Customizing: {edit_file}**")

                    with st.form(f"edit_{edit_file}"):
                        col_e1, col_e2, col_e3 = st.columns(3)

                        with col_e1:
                            config['skip_rows'] = st.number_input("Skip rows", 0, 5, config['skip_rows'], key=f"e_skip_{edit_file}")
                            config['date_column'] = st.number_input("Date col", 0, 10, config['date_column'], key=f"e_date_{edit_file}")

                        with col_e2:
                            config['header_row'] = st.number_input("Header row", 0, 5, config['header_row'], key=f"e_header_{edit_file}")
                            config['value_column'] = st.number_input("Value col", 0, 10, config['value_column'], key=f"e_value_{edit_file}")

                        with col_e3:
                            config['sensor_name'] = st.text_input("Sensor name", config['sensor_name'], key=f"e_sensor_{edit_file}")

                        if st.form_submit_button("Update This File"):
                            st.session_state.file_configs[edit_file] = config
                            st.success(f"✅ Updated {edit_file}")
                            st.rerun()

            st.markdown("---")

            # BIG CONTINUE BUTTON
            if st.button("🚀 Continue to Combine Data", type="primary", use_container_width=True):
                st.session_state.ready_to_combine = True
                st.rerun()

        # ========== STEP 4: COMBINE ==========
        if st.session_state.get('ready_to_combine'):
            st.markdown("---")
            st.header("Step 4: Combine All Data")

            if st.button("🔗 Start Combining", type="primary", use_container_width=True):
                with st.spinner("Combining all files..."):
                    progress = st.progress(0)

                    try:
                        loaded_dfs = []
                        total_files = len(st.session_state.file_configs)

                        # Load each file
                        for idx, (file_name, config) in enumerate(st.session_state.file_configs.items()):
                            progress.progress((idx + 1) / total_files)

                            file_path = st.session_state.uploaded_files[file_name]

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

                            # Extract columns
                            date_col = df_full.iloc[:, config['date_column']]
                            value_col = df_full.iloc[:, config['value_column']]

                            df_clean = pd.DataFrame({
                                'Date': date_col,
                                config['sensor_name']: value_col
                            })

                            # Excel Time
                            if config.get('excel_time_column') is not None:
                                excel_time_col = df_full.iloc[:, config['excel_time_column']]
                                df_clean['Excel Time'] = excel_time_col

                            # Parse dates
                            df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='mixed', errors='coerce')
                            df_clean = df_clean.dropna(subset=['Date'])

                            loaded_dfs.append(df_clean)

                        # Combine
                        combined = loaded_dfs[0].copy()
                        for df in loaded_dfs[1:]:
                            merge_cols = ['Date']
                            if 'Excel Time' in combined.columns and 'Excel Time' in df.columns:
                                combined = pd.merge(combined, df, on=merge_cols, how='outer', suffixes=('', '_dup'))
                                if 'Excel Time_dup' in combined.columns:
                                    combined['Excel Time'] = combined['Excel Time'].fillna(combined['Excel Time_dup'])
                                    combined = combined.drop(columns=['Excel Time_dup'])
                            else:
                                combined = pd.merge(combined, df, on=merge_cols, how='outer')

                        # Sort and reorder
                        combined = combined.sort_values('Date').reset_index(drop=True)

                        cols = ['Date']
                        if 'Excel Time' in combined.columns:
                            cols.append('Excel Time')
                        sensor_cols = [c for c in combined.columns if c not in ['Date', 'Excel Time']]
                        combined = combined[cols + sensor_cols]

                        if pd.api.types.is_datetime64tz_dtype(combined['Date']):
                            combined['Date'] = combined['Date'].dt.tz_localize(None)

                        st.session_state.combined_df = combined

                        st.success(f"✅ Successfully combined {total_files} files!")
                        st.balloons()

                        # Summary
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Rows", len(combined))
                        with col2:
                            st.metric("Sensors", len(combined.columns) - 1 - ('Excel Time' in combined.columns))
                        with col3:
                            st.metric("Columns", len(combined.columns))

                        st.markdown("### Preview")
                        st.dataframe(combined.head(20), use_container_width=True)

                    except Exception as e:
                        st.error(f"❌ Error: {str(e)}")
                        st.exception(e)

        # ========== STEP 5: EXPORT ==========
        if st.session_state.combined_df is not None:
            st.markdown("---")
            st.header("Step 5: Export")

            combined = st.session_state.combined_df

            output_filename = st.text_input(
                "Output filename",
                value=f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )

            if st.button("📥 Generate & Download CSV", type="primary", use_container_width=True):
                try:
                    output_dir = Path("output")
                    output_dir.mkdir(exist_ok=True)
                    output_path = output_dir / output_filename

                    # Format Date column
                    combined_export = combined.copy()
                    if 'Date' in combined_export.columns:
                        combined_export['Date'] = combined_export['Date'].dt.strftime('%m/%d/%Y %H:%M:%S')

                    combined_export.to_csv(output_path, index=False)

                    st.success(f"✅ Saved to: {output_path}")

                    with open(output_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Click to Download",
                            data=f,
                            file_name=output_filename,
                            mime='text/csv',
                            use_container_width=True
                        )

                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")


if __name__ == "__main__":
    main()
