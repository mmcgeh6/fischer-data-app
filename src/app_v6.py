"""
Fischer Energy Partners - Data Processing Application V6
Streamlined interface with parallel AI auto-detection for all files
- Removed Excel Time column support
- Simplified to single 'start_row' parameter
- Parallel AI analysis of all uploaded files
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
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
    page_title="Fischer Data Processing - V6",
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


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Data Processing - V6")
    st.markdown("**AI-Powered Parallel File Analysis** • Streamlined Configuration • No Excel Time")
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

        # ========== STEP 4: COMBINE DATA ==========
        if st.session_state.file_configs and len(st.session_state.file_configs) == len(st.session_state.uploaded_files):
            st.markdown("---")
            st.header("Step 4: Combine All Data")

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

                            # Sort by date
                            combined = combined.sort_values('Date').reset_index(drop=True)

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
                            st.markdown("### Data Preview")
                            st.dataframe(combined.head(20), use_container_width=True, height=400)

                    except Exception as e:
                        st.error(f"❌ Error combining files: {str(e)}")
                        st.exception(e)

        # ========== STEP 5: EXPORT ==========
        if st.session_state.combined_df is not None:
            st.markdown("---")
            st.header("Step 5: Export Combined Data")

            combined = st.session_state.combined_df

            col1, col2 = st.columns([2, 1])
            with col1:
                output_filename = st.text_input(
                    "Output filename",
                    value=f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )

            with col2:
                st.markdown("###")
                if st.button("📥 Generate CSV", type="primary", use_container_width=True):
                    try:
                        output_dir = Path("output")
                        output_dir.mkdir(exist_ok=True)
                        output_path = output_dir / output_filename

                        # Format for export
                        combined_export = combined.copy()
                        if 'Date' in combined_export.columns:
                            combined_export['Date'] = combined_export['Date'].dt.strftime('%m/%d/%Y %H:%M:%S')

                        combined_export.to_csv(output_path, index=False)

                        st.success(f"✅ File saved to: {output_path}")

                        # Download button
                        with open(output_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Download CSV File",
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
                st.write(combined.describe())

                st.markdown("### Missing Data Report")
                missing_data = combined.isnull().sum()
                missing_data = missing_data[missing_data > 0]
                if not missing_data.empty:
                    st.write(missing_data)
                else:
                    st.success("No missing data!")

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
