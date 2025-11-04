"""
Fischer Energy Partners - Data Processing Application V7
Quarter-Hour Resampling with Quality Flags
- All V6 features (parallel AI, timestamp normalization)
- Raw merged CSV download
- 15-minute interval resampling with merge_asof
- Inexact match and stale data flagging
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import json
import os
import io
from dotenv import load_dotenv
from anthropic import Anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from timestamp_normalizer import format_timestamp_mdy_hms, detect_timestamp_format

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing - V7",
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
if 'resampled_df' not in st.session_state:
    st.session_state.resampled_df = None
if 'resampling_stats' not in st.session_state:
    st.session_state.resampling_stats = {}
if 'ai_debug_log' not in st.session_state:
    st.session_state.ai_debug_log = []
if 'ai_analysis_complete' not in st.session_state:
    st.session_state.ai_analysis_complete = False


def read_raw_lines(file_path, num_lines=15):
    """Read first N lines of a file as raw text. For Excel files, convert to CSV-like format."""
    lines = []

    # Check if it's an Excel file
    if str(file_path).lower().endswith(('.xlsx', '.xls')):
        try:
            df = pd.read_excel(file_path, header=None, nrows=num_lines)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_text = csv_buffer.getvalue()
            lines = csv_text.strip().split('\n')
            return lines[:num_lines]
        except Exception as e:
            return [f"Error reading Excel file: {str(e)}"]

    # For CSV/text files
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                lines.append(line.rstrip())
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                lines.append(line.rstrip())
    return lines


def build_ai_prompt(file_name, text_sample):
    """Build the AI prompt for column detection."""
    return f"""Analyze the first 15 lines of this CSV/Excel file sample to determine the column configuration for data processing.

File name: {file_name or 'unknown'}

Raw file content (first 15 lines):
{text_sample}

Based on this data, identify:
1. The delimiter used (comma, tab, semicolon, etc.)
2. Which row index (0-based) contains the column headers and is the START of data
3. Which column index (0-based) contains the date/timestamp
4. Which column index (0-based) contains the sensor value/reading
5. A sensor name (extract from metadata or suggest based on filename)

Return ONLY a JSON object in this exact format with no additional text:
{{
  "delimiter": ",",
  "start_row": 1,
  "date_column": 0,
  "value_column": 2,
  "sensor_name": "sensor name here"
}}

Rules:
- All indices MUST be 0-based
- start_row is where column headers are located (data begins on start_row+1)
- If a column doesn't exist, use -1 as the value
- Return only valid JSON with no explanations or additional text"""


def call_claude_api(prompt, api_key):
    """Call Claude API and return the response text."""
    client = Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1500,
            temperature=0.6,
            messages=[{"role": "user", "content": prompt}]
        )

        # Extract text from response
        response_text = response.content[0].text.strip()
        return response_text
    except Exception as e:
        raise RuntimeError(f"Claude API call failed: {str(e)}")


def analyze_single_file(file_name, file_path, api_key):
    """Analyze a single file with AI and return the configuration."""
    debug_entry = {
        'file_name': file_name,
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'request': None,
        'response': None,
        'error': None,
        'success': False
    }

    try:
        # Read first 15 lines
        raw_lines = read_raw_lines(file_path, num_lines=15)
        text_sample = "\n".join(raw_lines)

        # Build prompt
        prompt = build_ai_prompt(file_name, text_sample)

        # Store request
        debug_entry['request'] = {
            'model': 'claude-sonnet-4-5-20250929',
            'max_tokens': 1500,
            'temperature': 0.6,
            'prompt_length': len(prompt),
            'prompt_preview': prompt[:500] + '...' if len(prompt) > 500 else prompt,
            'raw_text_lines': raw_lines[:5]
        }

        # Call API
        response_text = call_claude_api(prompt, api_key)

        # Store response
        debug_entry['response'] = {
            'raw_text': response_text,
            'response_length': len(response_text)
        }

        # Parse JSON
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        if json_start != -1 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            config = json.loads(json_str)

            debug_entry['response']['parsed_json'] = config
            debug_entry['success'] = True

            return config, debug_entry
        else:
            debug_entry['error'] = "Could not find JSON in response"
            return None, debug_entry

    except json.JSONDecodeError as e:
        debug_entry['error'] = f"JSON decode error: {str(e)}"
        return None, debug_entry
    except Exception as e:
        debug_entry['error'] = f"Error: {str(e)}"
        return None, debug_entry


def analyze_all_files_parallel(uploaded_files, api_key):
    """Analyze all uploaded files in parallel using ThreadPoolExecutor."""
    configs = {}
    debug_logs = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(analyze_single_file, file_name, file_path, api_key): file_name
            for file_name, file_path in uploaded_files.items()
        }

        # Collect results as they complete
        for future in as_completed(future_to_file):
            file_name = future_to_file[future]
            try:
                config, debug_entry = future.result()
                if config:
                    configs[file_name] = config
                debug_logs.append(debug_entry)
            except Exception as e:
                debug_logs.append({
                    'file_name': file_name,
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'error': f"Exception: {str(e)}",
                    'success': False
                })

    return configs, debug_logs


def parse_file_with_config(file_path, start_row=0, delimiter=',', num_rows=10):
    """Parse file using the provided configuration."""
    try:
        if str(file_path).lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path, header=start_row, nrows=num_rows)
        else:
            df = pd.read_csv(
                file_path,
                sep=delimiter,
                header=start_row,
                nrows=num_rows,
                encoding='utf-8',
                encoding_errors='ignore',
                on_bad_lines='skip'
            )
        return df
    except Exception as e:
        return None


def resample_to_quarter_hour(combined_df, tolerance_minutes=2):
    """
    Resample combined data to 15-minute intervals with PER-SENSOR nearest-value matching.

    Each sensor independently finds its closest value within ±2 minutes of each quarter-hour mark.
    This allows different sensors to pull from different source timestamps for the same target time.

    Args:
        combined_df: The merged dataframe with all sensors
        tolerance_minutes: Window for finding nearest match (±2 minutes default)

    Returns:
        Tuple of (resampled_df, stats_dict)
    """
    if combined_df is None or combined_df.empty:
        return None, {}

    # Create complete range of 15-minute timestamps
    start_time = combined_df['Date'].min()
    end_time = combined_df['Date'].max()

    # Round start to previous 15-min mark
    start_time = start_time.replace(minute=(start_time.minute // 15) * 15, second=0, microsecond=0)

    # Round end to next 15-min mark
    end_minute = ((end_time.minute // 15) + 1) * 15
    if end_minute >= 60:
        end_time = end_time + timedelta(hours=1)
        end_time = end_time.replace(minute=0, second=0, microsecond=0)
    else:
        end_time = end_time.replace(minute=end_minute, second=0, microsecond=0)

    # Generate 15-minute interval timestamps
    target_timestamps = pd.date_range(start=start_time, end=end_time, freq='15min')

    # Get sensor columns (exclude Date)
    sensor_cols = [col for col in combined_df.columns if col != 'Date']

    # Initialize result lists
    result_data = {'Date': target_timestamps}
    for sensor in sensor_cols:
        result_data[sensor] = []
        result_data[f'{sensor}_Inexact_Flag'] = []

    tolerance = pd.Timedelta(minutes=tolerance_minutes)

    # For each target timestamp, find nearest value for each sensor independently
    for target_time in target_timestamps:
        # Calculate time differences for all rows
        time_diffs = (combined_df['Date'] - target_time).abs()
        within_window = time_diffs <= tolerance

        # For each sensor, find its nearest value
        for sensor in sensor_cols:
            # Filter to rows within tolerance that have a non-null value for this sensor
            valid_mask = within_window & combined_df[sensor].notna()

            if not valid_mask.any():
                # No value within window - use NULL
                result_data[sensor].append(None)
                result_data[f'{sensor}_Inexact_Flag'].append(False)
            else:
                # Find the row with the smallest time difference that has this sensor's value
                valid_time_diffs = time_diffs[valid_mask]
                closest_idx = valid_time_diffs.idxmin()

                # Extract the value (preserve 0 as 0, not NULL)
                value = combined_df.loc[closest_idx, sensor]
                result_data[sensor].append(value)

                # Check if the source timestamp is exactly on the quarter-hour mark
                source_time = combined_df.loc[closest_idx, 'Date']
                is_exact = (source_time.minute % 15 == 0) and (source_time.second == 0)
                result_data[f'{sensor}_Inexact_Flag'].append(not is_exact)

    # Create DataFrame
    resampled = pd.DataFrame(result_data)

    # Flag stale data (4+ consecutive identical values per sensor)
    stale_flags = {}
    for sensor in sensor_cols:
        flag_col = f'{sensor}_Stale_Flag'
        # Check if current value equals previous 3 values (4 consecutive identical)
        is_stale = (
            (resampled[sensor] == resampled[sensor].shift(1)) &
            (resampled[sensor] == resampled[sensor].shift(2)) &
            (resampled[sensor] == resampled[sensor].shift(3)) &
            (resampled[sensor].notna())  # Don't flag NaN values as stale
        )
        resampled[flag_col] = is_stale
        stale_flags[sensor] = int(is_stale.sum())

    # Calculate statistics
    inexact_flag_cols = [col for col in resampled.columns if col.endswith('_Inexact_Flag')]
    total_inexact = sum(resampled[col].sum() for col in inexact_flag_cols)

    stats = {
        'total_intervals': len(resampled),
        'total_inexact_matches': int(total_inexact),
        'stale_by_sensor': stale_flags,
        'total_stale': sum(stale_flags.values()),
        'date_range': {
            'start': resampled['Date'].min(),
            'end': resampled['Date'].max()
        }
    }

    return resampled, stats


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Data Processing - V7")
    st.markdown("**Quarter-Hour Resampling** • AI Analysis • Quality Flags • Timestamp Normalization")
    st.markdown("---")

    # ========== STEP 1: UPLOAD ==========
    st.header("Step 1: Upload Files")

    uploaded_files = st.file_uploader(
        "📁 Upload your sensor files",
        type=['xlsx', 'csv', 'xls'],
        accept_multiple_files=True,
        help="Select all files (10-90 files)"
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

        # ========== STEP 2: AI ANALYSIS ==========
        st.header("Step 2: AI Analysis")

        # Check for API key
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            st.error("⚠️ CLAUDE_API_KEY not found in .env file. Please add your API key.")
            st.code("CLAUDE_API_KEY=sk-ant-...", language="bash")
        else:
            col1, col2 = st.columns([2, 1])

            with col1:
                st.info("💡 Click below to analyze ALL files in parallel with Claude AI")

            with col2:
                if st.button("🤖 Analyze All Files", type="primary", use_container_width=True):
                    with st.spinner(f"Analyzing {len(st.session_state.uploaded_files)} files in parallel..."):
                        progress_bar = st.progress(0)

                        # Run parallel analysis
                        configs, debug_logs = analyze_all_files_parallel(
                            st.session_state.uploaded_files,
                            api_key
                        )

                        progress_bar.progress(100)

                        # Store results
                        st.session_state.file_configs = configs
                        st.session_state.ai_debug_log = debug_logs
                        st.session_state.ai_analysis_complete = True

                        # Show summary
                        success_count = sum(1 for log in debug_logs if log.get('success', False))
                        st.success(f"✅ Analysis complete! {success_count}/{len(debug_logs)} files configured successfully")
                        st.rerun()

            # Show analysis results if complete
            if st.session_state.ai_analysis_complete and st.session_state.file_configs:
                st.markdown("---")
                st.subheader("Analysis Results")

                # Summary metrics
                total_files = len(st.session_state.uploaded_files)
                configured_files = len(st.session_state.file_configs)
                success_rate = (configured_files / total_files * 100) if total_files > 0 else 0

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Files", total_files)
                with col2:
                    st.metric("Configured", configured_files)
                with col3:
                    st.metric("Success Rate", f"{success_rate:.0f}%")

        # ========== STEP 3: REVIEW & EDIT CONFIGS ==========
        if st.session_state.file_configs:
            st.markdown("---")
            st.header("Step 3: Review & Edit Configurations")

            st.info("💡 Review AI-detected settings below. Edit if needed before combining.")

            for idx, (file_name, file_path) in enumerate(st.session_state.uploaded_files.items()):
                if idx > 0:
                    st.markdown("---")

                config = st.session_state.file_configs.get(file_name, {})

                st.markdown(f"### 📄 {file_name}")

                # Configuration inputs
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    start_row = st.number_input(
                        "Start Row",
                        min_value=0,
                        max_value=10,
                        value=config.get('start_row', 0),
                        key=f"start_{file_name}",
                        help="Row where column headers are located"
                    )

                with col2:
                    delimiter_options = [',', '\t', ';', '|']
                    delimiter_labels = ['Comma (,)', 'Tab (\\t)', 'Semicolon (;)', 'Pipe (|)']
                    default_delim = config.get('delimiter', ',')
                    try:
                        delim_idx = delimiter_options.index(default_delim)
                    except ValueError:
                        delim_idx = 0

                    delimiter = st.selectbox(
                        "Delimiter",
                        options=delimiter_options,
                        format_func=lambda x: delimiter_labels[delimiter_options.index(x)],
                        index=delim_idx,
                        key=f"delim_{file_name}"
                    )

                with col3:
                    date_column = st.number_input(
                        "Date Column",
                        min_value=0,
                        max_value=20,
                        value=config.get('date_column', 0),
                        key=f"date_{file_name}",
                        help="Column index for date/timestamp (0-based)"
                    )

                with col4:
                    value_column = st.number_input(
                        "Value Column",
                        min_value=0,
                        max_value=20,
                        value=config.get('value_column', 2),
                        key=f"value_{file_name}",
                        help="Column index for sensor values (0-based)"
                    )

                # Sensor name
                sensor_name = st.text_input(
                    "Sensor Name",
                    value=config.get('sensor_name', Path(file_name).stem),
                    key=f"sensor_{file_name}"
                )

                # Update config in session state
                st.session_state.file_configs[file_name] = {
                    'start_row': start_row,
                    'delimiter': delimiter,
                    'date_column': date_column,
                    'value_column': value_column,
                    'sensor_name': sensor_name
                }

                # Show extracted data preview immediately (not in expander)
                st.markdown("**📊 Extracted Data Preview** (what will be used in processing):")
                df_preview = parse_file_with_config(file_path, start_row, delimiter, num_rows=10)
                if df_preview is not None and not df_preview.empty:
                    # Create a preview of just the extracted columns
                    extracted_preview = pd.DataFrame()

                    # Show selected columns info
                    col_info1, col_info2 = st.columns(2)
                    with col_info1:
                        if date_column < len(df_preview.columns):
                            date_col_name = df_preview.columns[date_column]
                            st.success(f"✅ Date Column [{date_column}]: `{date_col_name}`")
                            extracted_preview['Date'] = df_preview.iloc[:, date_column]
                        else:
                            st.error(f"❌ Date column {date_column} doesn't exist!")

                    with col_info2:
                        if value_column < len(df_preview.columns):
                            value_col_name = df_preview.columns[value_column]
                            st.success(f"✅ Value Column [{value_column}]: `{value_col_name}`")
                            extracted_preview[sensor_name] = df_preview.iloc[:, value_column]
                        else:
                            st.error(f"❌ Value column {value_column} doesn't exist!")

                    # Show timestamp conversion preview
                    if date_column < len(df_preview.columns) and not extracted_preview.empty:
                        st.markdown("**🕐 Timestamp Conversion:**")
                        # Get first non-null timestamp
                        sample_ts = extracted_preview['Date'].dropna().iloc[0] if len(extracted_preview['Date'].dropna()) > 0 else None
                        if sample_ts is not None:
                            try:
                                original_str = str(sample_ts)
                                normalized = format_timestamp_mdy_hms(original_str)

                                col_ts1, col_ts2, col_ts3 = st.columns([2, 1, 2])
                                with col_ts1:
                                    st.text(f"Original: {original_str}")
                                with col_ts2:
                                    st.text("→")
                                with col_ts3:
                                    st.success(f"Standardized: {normalized}")
                            except Exception as e:
                                st.caption(f"Timestamp will be normalized during combine step")

                    # Show the extracted data table
                    if not extracted_preview.empty:
                        st.dataframe(extracted_preview, use_container_width=True, height=250)
                    else:
                        st.warning("No valid columns selected for extraction")
                else:
                    st.error("Could not parse file with current settings")

                # Keep raw file preview in collapsible expander
                with st.expander("🔍 View Raw File Data (all columns)", expanded=False):
                    if df_preview is not None and not df_preview.empty:
                        st.caption("This shows ALL columns from the original file. Only the selected columns above will be used in processing.")
                        st.dataframe(df_preview, use_container_width=True, height=300)
                    else:
                        st.error("Could not parse file")

        # ========== STEP 4: COMBINE DATA (RAW MERGE) ==========
        if st.session_state.file_configs and len(st.session_state.file_configs) == len(st.session_state.uploaded_files):
            st.markdown("---")
            st.header("Step 4: Combine All Data (Raw Merge)")

            # Show timestamp normalization preview
            st.info("🕐 **Timestamp Standardization**: All timestamps will be normalized to **MM/DD/YYYY HH:MM:SS** format")

            with st.expander("📅 Preview Timestamp Conversion", expanded=False):
                st.markdown("**Sample conversions from your uploaded files:**")

                # Show a few examples from each file
                for file_name, config in list(st.session_state.file_configs.items())[:3]:  # Show first 3 files
                    file_path = st.session_state.uploaded_files[file_name]
                    df_sample = parse_file_with_config(file_path, config['start_row'], config['delimiter'], num_rows=3)

                    if df_sample is not None and config['date_column'] < len(df_sample.columns):
                        st.markdown(f"**{file_name}:**")

                        # Get first non-null timestamp
                        date_col_name = df_sample.columns[config['date_column']]
                        sample_timestamps = df_sample[date_col_name].dropna().head(2)

                        for original_ts in sample_timestamps:
                            try:
                                original_str = str(original_ts)
                                detected_format = detect_timestamp_format(original_str)
                                normalized = format_timestamp_mdy_hms(original_str)

                                col1, col2, col3 = st.columns([2, 1, 2])
                                with col1:
                                    st.text(f"Original: {original_str}")
                                with col2:
                                    st.text("→")
                                with col3:
                                    st.success(f"Standardized: {normalized}")

                                st.caption(f"Detected: {detected_format}")
                                st.markdown("---")
                            except Exception as e:
                                st.warning(f"Could not parse: {original_ts} ({str(e)})")

            if st.button("🔗 Combine All Files", type="primary", use_container_width=True):
                with st.spinner("Combining all files..."):
                    progress = st.progress(0)

                    try:
                        loaded_dfs = []
                        total_files = len(st.session_state.file_configs)

                        for idx, (file_name, config) in enumerate(st.session_state.file_configs.items()):
                            progress.progress((idx + 1) / total_files)

                            file_path = st.session_state.uploaded_files[file_name]

                            # Read full file
                            if file_path.lower().endswith(('.xlsx', '.xls')):
                                df_full = pd.read_excel(
                                    file_path,
                                    header=config['start_row']
                                )
                            else:
                                df_full = pd.read_csv(
                                    file_path,
                                    sep=config['delimiter'],
                                    header=config['start_row'],
                                    encoding='utf-8',
                                    encoding_errors='ignore',
                                    on_bad_lines='skip'
                                )

                            # Extract required columns
                            df_clean = pd.DataFrame()

                            # Get date column
                            if config['date_column'] < len(df_full.columns):
                                date_col_name = df_full.columns[config['date_column']]
                                df_clean['Date'] = df_full[date_col_name]

                            # Get value column
                            if config['value_column'] < len(df_full.columns):
                                value_col_name = df_full.columns[config['value_column']]
                                df_clean[config['sensor_name']] = df_full[value_col_name]

                            # Normalize and standardize timestamps
                            if 'Date' in df_clean.columns:
                                # Apply timestamp normalization
                                normalized_dates = []
                                for ts_val in df_clean['Date']:
                                    try:
                                        # Normalize to MM/DD/YYYY HH:MM:SS format
                                        normalized = format_timestamp_mdy_hms(str(ts_val))
                                        # Parse back to datetime for consistency
                                        normalized_dates.append(pd.to_datetime(normalized, format='%m/%d/%Y %H:%M:%S'))
                                    except Exception:
                                        # If normalization fails, try pandas default parsing
                                        try:
                                            normalized_dates.append(pd.to_datetime(ts_val, format='mixed', errors='coerce'))
                                        except:
                                            normalized_dates.append(pd.NaT)

                                df_clean['Date'] = normalized_dates
                                df_clean = df_clean.dropna(subset=['Date'])

                                # Remove timezone if present (we standardized to America/New_York)
                                if pd.api.types.is_datetime64tz_dtype(df_clean['Date']):
                                    df_clean['Date'] = df_clean['Date'].dt.tz_localize(None)

                                loaded_dfs.append(df_clean)

                        # Combine all dataframes
                        if loaded_dfs:
                            combined = loaded_dfs[0].copy()
                            for df in loaded_dfs[1:]:
                                combined = pd.merge(combined, df, on='Date', how='outer')

                            # Sort by date and remove exact duplicates
                            combined = combined.sort_values('Date').reset_index(drop=True)
                            combined = combined.drop_duplicates()

                            st.session_state.combined_df = combined

                            st.success(f"✅ Successfully combined {total_files} files!")
                            st.balloons()

                            # Show summary metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Rows", f"{len(combined):,}")
                            with col2:
                                num_sensors = len([c for c in combined.columns if c != 'Date'])
                                st.metric("Sensors", num_sensors)
                            with col3:
                                date_range = f"{combined['Date'].min().strftime('%m/%d/%Y')} - {combined['Date'].max().strftime('%m/%d/%Y')}"
                                st.metric("Date Range", date_range)

                            # Show preview
                            st.markdown("### Raw Merged Data Preview")
                            st.dataframe(combined.head(20), use_container_width=True, height=400)

                            st.info("💡 **This is raw merged data.** Proceed to Step 5 to resample to 15-minute intervals with quality flags.")

                    except Exception as e:
                        st.error(f"❌ Error combining files: {str(e)}")
                        st.exception(e)

        # ========== DOWNLOAD RAW MERGED CSV ==========
        if st.session_state.combined_df is not None and st.session_state.resampled_df is None:
            st.markdown("---")
            st.subheader("📥 Download Raw Merged Data (Optional)")

            st.info("💾 You can download the raw merged data before resampling. This preserves all original timestamps.")

            col1, col2 = st.columns([2, 1])
            with col1:
                raw_filename = st.text_input(
                    "Raw merged filename",
                    value=f"raw_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    key="raw_filename"
                )

            with col2:
                st.markdown("###")
                # Generate download button
                combined = st.session_state.combined_df
                combined_export = combined.copy()
                if 'Date' in combined_export.columns:
                    combined_export['Date'] = combined_export['Date'].dt.strftime('%m/%d/%Y %H:%M:%S')

                csv_data = combined_export.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download Raw Merged CSV",
                    data=csv_data,
                    file_name=raw_filename,
                    mime='text/csv',
                    use_container_width=True
                )

        # ========== STEP 5: RESAMPLE TO 15-MINUTE INTERVALS ==========
        if st.session_state.combined_df is not None:
            st.markdown("---")
            st.header("Step 5: Resample to Quarter-Hour Intervals")

            st.info("""
            🕐 **Quarter-Hour Resampling Process (Per-Sensor Matching):**
            - Creates complete 15-minute interval timestamps (:00, :15, :30, :45)
            - **Each sensor independently** finds its closest value within ±2 minute tolerance
            - Different sensors can pull from different source timestamps
            - Flags inexact matches per sensor (timestamps not exactly on 15-min marks)
            - Flags stale sensor data (4+ consecutive identical readings)
            - Uses NULL when no value exists within ±2 minute window
            """)

            if st.button("⏱️ Resample to 15-Minute Intervals", type="primary", use_container_width=True):
                with st.spinner("Resampling to quarter-hour intervals..."):
                    try:
                        resampled, stats = resample_to_quarter_hour(st.session_state.combined_df)

                        if resampled is not None:
                            st.session_state.resampled_df = resampled
                            st.session_state.resampling_stats = stats

                            st.success("✅ Successfully resampled to 15-minute intervals!")
                            st.balloons()

                            # Show statistics
                            st.markdown("### Resampling Statistics")

                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total Intervals", f"{stats['total_intervals']:,}")
                            with col2:
                                st.metric("Inexact Matches (All Sensors)", f"{stats['total_inexact_matches']:,}")
                            with col3:
                                st.metric("Total Stale Flags", stats['total_stale'])

                            # Stale data by sensor
                            if stats['stale_by_sensor']:
                                st.markdown("#### Stale Data by Sensor")
                                stale_df = pd.DataFrame([
                                    {'Sensor': sensor, 'Stale Count': count}
                                    for sensor, count in stats['stale_by_sensor'].items()
                                ]).sort_values('Stale Count', ascending=False)
                                st.dataframe(stale_df, use_container_width=True)

                            # Show preview
                            st.markdown("### Resampled Data Preview")
                            st.dataframe(resampled.head(20), use_container_width=True, height=400)

                        else:
                            st.error("❌ Failed to resample data")

                    except Exception as e:
                        st.error(f"❌ Error during resampling: {str(e)}")
                        st.exception(e)

        # ========== STEP 6: EXPORT RESAMPLED DATA ==========
        if st.session_state.resampled_df is not None:
            st.markdown("---")
            st.header("Step 6: Export Resampled Data")

            resampled = st.session_state.resampled_df

            col1, col2 = st.columns([2, 1])
            with col1:
                output_filename = st.text_input(
                    "Output filename",
                    value=f"resampled_15min_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )

            with col2:
                st.markdown("###")
                if st.button("📥 Generate Resampled CSV", type="primary", use_container_width=True):
                    try:
                        output_dir = Path("output")
                        output_dir.mkdir(exist_ok=True)
                        output_path = output_dir / output_filename

                        # Format for export
                        resampled_export = resampled.copy()
                        if 'Date' in resampled_export.columns:
                            resampled_export['Date'] = resampled_export['Date'].dt.strftime('%m/%d/%Y %H:%M:%S')

                        resampled_export.to_csv(output_path, index=False)

                        st.success(f"✅ File saved to: {output_path}")

                        # Download button
                        with open(output_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Download Resampled CSV",
                                data=f,
                                file_name=output_filename,
                                mime='text/csv',
                                use_container_width=True
                            )

                    except Exception as e:
                        st.error(f"❌ Error creating file: {str(e)}")

            # Export statistics
            with st.expander("📊 Export Statistics"):
                st.markdown("### Data Summary")

                # Get sensor columns (exclude Date and flag columns)
                sensor_cols = [c for c in resampled.columns
                              if c != 'Date'
                              and not c.endswith('_Inexact_Flag')
                              and not c.endswith('_Stale_Flag')]

                if sensor_cols:
                    st.write(resampled[sensor_cols].describe())

                st.markdown("### Quality Flags Summary")

                # Inexact match flags (per sensor)
                inexact_flag_cols = [c for c in resampled.columns if c.endswith('_Inexact_Flag')]
                if inexact_flag_cols:
                    st.write("**Inexact Match Flags by Sensor:**")
                    for col in inexact_flag_cols:
                        sensor_name = col.replace('_Inexact_Flag', '')
                        inexact_count = resampled[col].sum()
                        inexact_pct = (inexact_count / len(resampled)) * 100
                        st.write(f"- {sensor_name}: {inexact_count:,} ({inexact_pct:.1f}%)")

                # Stale data flags (per sensor)
                stale_flag_cols = [c for c in resampled.columns if c.endswith('_Stale_Flag')]
                if stale_flag_cols:
                    st.write("**Stale Data Flags by Sensor:**")
                    for col in stale_flag_cols:
                        sensor_name = col.replace('_Stale_Flag', '')
                        stale_count = resampled[col].sum()
                        st.write(f"- {sensor_name}: {stale_count:,}")

                st.markdown("### Missing Data Report")
                missing_data = resampled[sensor_cols].isnull().sum()
                missing_data = missing_data[missing_data > 0]
                if not missing_data.empty:
                    st.write(missing_data)
                else:
                    st.success("No missing data in sensor columns!")

        # ========== AI DEBUG PANEL (ALWAYS VISIBLE) ==========
        st.markdown("---")
        st.header("🔍 AI Debug Panel")

        if not st.session_state.ai_debug_log:
            st.info("No AI analysis yet. Upload files and click 'Analyze All Files' to see debug information.")
        else:
            # Summary
            total_calls = len(st.session_state.ai_debug_log)
            successful_calls = sum(1 for log in st.session_state.ai_debug_log if log.get('success', False))

            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.metric("Total API Calls", total_calls)
            with col2:
                st.metric("Successful", f"{successful_calls}/{total_calls}")
            with col3:
                if st.button("🗑️ Clear Log"):
                    st.session_state.ai_debug_log = []
                    st.rerun()

            st.markdown("---")

            # Display each debug entry
            for idx, entry in enumerate(st.session_state.ai_debug_log):
                with st.expander(f"📄 {entry['file_name']} - {'✅ Success' if entry.get('success') else '❌ Failed'}", expanded=False):
                    col1, col2 = st.columns([1, 1])

                    with col1:
                        st.markdown("**⏰ Timestamp**")
                        st.text(entry['timestamp'])

                        if entry.get('request'):
                            st.markdown("**📤 Request Details**")
                            st.json({
                                'model': entry['request']['model'],
                                'max_tokens': entry['request']['max_tokens'],
                                'temperature': entry['request']['temperature'],
                                'prompt_length': entry['request']['prompt_length']
                            })

                    with col2:
                        if entry.get('response'):
                            st.markdown("**📥 Response**")
                            if 'parsed_json' in entry['response']:
                                st.json(entry['response']['parsed_json'])
                            else:
                                st.code(entry['response'].get('raw_text', 'No response')[:500])

                        if entry.get('error'):
                            st.markdown("**❌ Error**")
                            st.error(entry['error'])

                    # Full prompt and response
                    if entry.get('request'):
                        with st.expander("View Full Prompt"):
                            st.code(entry['request'].get('prompt_preview', ''), language='text')

                            st.markdown("**First 5 Lines Sent:**")
                            for line_idx, line in enumerate(entry['request'].get('raw_text_lines', [])):
                                st.text(f"{line_idx}: {line[:100]}")

                    if entry.get('response'):
                        with st.expander("View Full Response"):
                            st.code(entry['response'].get('raw_text', ''))


if __name__ == "__main__":
    main()
