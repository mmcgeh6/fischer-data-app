"""
Fischer Energy Partners - Data Processing Application V5
Improved interface with column dropdowns and auto-detection
Now with Claude AI for intelligent file configuration
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import warnings
import json
import os
import io
from dotenv import load_dotenv
from anthropic import Anthropic

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing - V5",
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
if 'preview_data' not in st.session_state:
    st.session_state.preview_data = {}
if 'column_mappings' not in st.session_state:
    st.session_state.column_mappings = {}
if 'ai_configs' not in st.session_state:
    st.session_state.ai_configs = {}
if 'ai_debug_log' not in st.session_state:
    st.session_state.ai_debug_log = []


def auto_detect_column_type(column_data, header_name=None):
    """Auto-detect what type of data is in a column."""
    # First check the header name if provided
    if header_name:
        header_lower = str(header_name).lower().strip()

        # Check for Date column
        if any(keyword in header_lower for keyword in ['date', 'datetime', 'timestamp', 'time stamp']):
            return "Date"

        # Check for Excel Time column
        if 'excel' in header_lower and 'time' in header_lower:
            return "Excel Time"
        elif header_lower == 'excel time':
            return "Excel Time"

        # Check for Value column
        if any(keyword in header_lower for keyword in ['value', 'reading', 'measurement', 'temp', 'temperature',
                                                        'pressure', 'flow', 'vfd', 'output', 'input', 'sensor']):
            return "Value"

        # Check for Notes column
        if any(keyword in header_lower for keyword in ['note', 'notes', 'comment', 'description', 'remark']):
            return "Notes"

    # If header analysis didn't work, analyze the data content
    # Convert to string and clean
    str_data = column_data.astype(str).str.strip()

    # Remove null/empty values for analysis
    non_empty = str_data[str_data != ''].head(10)

    if len(non_empty) == 0:
        return "Ignore"

    # Check for Date patterns in data
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # MM/DD/YYYY or MM-DD-YYYY
        r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',     # YYYY-MM-DD
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\s+\d{1,2}:\d{2}',  # Date with time
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',  # ISO format
    ]

    for pattern in date_patterns:
        if non_empty.str.contains(pattern, regex=True, na=False).any():
            return "Date"

    # Check for Excel Time (decimal numbers around 45000-46000)
    try:
        numeric_vals = pd.to_numeric(non_empty, errors='coerce')
        if not numeric_vals.isna().all():
            mean_val = numeric_vals.mean()
            if 40000 < mean_val < 50000 and '.' in str(non_empty.iloc[0]):
                return "Excel Time"
    except:
        pass

    # Check for Value (numeric data)
    try:
        numeric_vals = pd.to_numeric(non_empty, errors='coerce')
        if numeric_vals.notna().sum() / len(non_empty) > 0.5:  # More than 50% are numeric
            return "Value"
    except:
        pass

    # Check if it looks like notes/text
    if non_empty.str.len().mean() > 20:
        return "Notes"

    return "Ignore"


def detect_columns_with_ai(raw_text_lines, file_name=None):
    """Use Claude AI to automatically detect column configuration from raw file preview."""
    debug_entry = {
        'file_name': file_name,
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'request': None,
        'response': None,
        'error': None,
        'success': False
    }

    try:
        # Initialize Claude client
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            st.warning("Claude API key not found in .env file")
            debug_entry['error'] = "API key not found"
            st.session_state.ai_debug_log.append(debug_entry)
            return None

        client = Anthropic(api_key=api_key)

        # Prepare the text sample for Claude
        text_sample = "\n".join(raw_text_lines[:15])  # Send first 15 lines

        # Create prompt for Claude
        prompt = f"""Analyze this CSV/Excel file sample and determine the column configuration for data processing.

File name: {file_name or 'unknown'}

Raw file content (first 15 lines):
{text_sample}

Based on this data, identify:
1. How many rows to skip at the beginning (typically 0 or 1 for metadata header)
2. Which row contains the column headers (after skipping)
3. The delimiter used (comma, tab, semicolon, etc.)
4. Which column index contains the date/timestamp
5. Which column index contains the Excel Time (if present)
6. Which column index contains the sensor value/reading
7. A sensor name (extract from metadata or suggest based on filename)

Return ONLY a JSON object in this exact format with no additional text:
{{
  "skip_rows": 1,
  "header_row": 0,
  "delimiter": ",",
  "date_column": 0,
  "excel_time_column": 1,
  "value_column": 2,
  "sensor_name": "sensor name here"
}}

If a column doesn't exist (like excel_time_column), use -1 as the value.
Column indices should be 0-based."""

        # Store the request in debug log
        debug_entry['request'] = {
            'model': 'claude-sonnet-4-5-20250929',
            'max_tokens': 1500,
            'temperature': 0.6,
            'prompt_length': len(prompt),
            'prompt_preview': prompt[:500] + '...' if len(prompt) > 500 else prompt,
            'raw_text_lines': raw_text_lines[:5]  # First 5 lines for debug
        }

        # Call Claude API
        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",  # Using Haiku for speed and cost efficiency
            max_tokens=1500,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.6  # Use deterministic output
        )

        # Extract JSON from response
        response_text = response.content[0].text.strip()

        # Store the response in debug log
        debug_entry['response'] = {
            'raw_text': response_text,
            'response_length': len(response_text)
        }

        # Find JSON in response (in case Claude adds any text)
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        if json_start != -1 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            config = json.loads(json_str)

            # Store parsed config in debug
            debug_entry['response']['parsed_json'] = config
            debug_entry['success'] = True

            # Add to debug log
            st.session_state.ai_debug_log.append(debug_entry)

            return config
        else:
            st.error("Could not parse AI response")
            debug_entry['error'] = "Could not find JSON in response"
            st.session_state.ai_debug_log.append(debug_entry)
            return None

    except json.JSONDecodeError as e:
        st.error(f"Error parsing AI response as JSON: {e}")
        debug_entry['error'] = f"JSON decode error: {str(e)}"
        st.session_state.ai_debug_log.append(debug_entry)
        return None
    except Exception as e:
        st.error(f"Error calling Claude AI: {e}")
        debug_entry['error'] = f"API call error: {str(e)}"
        st.session_state.ai_debug_log.append(debug_entry)
        return None


def read_raw_text(file_path, num_lines=15):
    """Read first N lines of a file as raw text. For Excel files, convert to CSV-like format first."""
    lines = []

    # Check if it's an Excel file
    if str(file_path).lower().endswith(('.xlsx', '.xls')):
        try:
            # Read Excel file and convert to CSV-like text
            import io
            df = pd.read_excel(file_path, header=None, nrows=num_lines)

            # Convert to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_text = csv_buffer.getvalue()

            # Split into lines
            lines = csv_text.strip().split('\n')
            return lines[:num_lines]
        except Exception as e:
            # If Excel reading fails, return error message
            return [f"Error reading Excel file: {str(e)}"]

    # For CSV/text files, read as text
    try:
        # Try UTF-8 first
        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                lines.append(line.rstrip())
    except UnicodeDecodeError:
        # Fallback to latin-1
        with open(file_path, 'r', encoding='latin-1') as f:
            for i, line in enumerate(f):
                if i >= num_lines:
                    break
                lines.append(line.rstrip())
    return lines


def detect_header_row(file_path, max_rows=10):
    """Detect which row contains the column headers."""
    try:
        # Read first N rows
        if str(file_path).endswith('.csv'):
            df = pd.read_csv(file_path, nrows=max_rows, header=None,
                           encoding='utf-8', encoding_errors='ignore')
        else:
            df = pd.read_excel(file_path, nrows=max_rows, header=None)

        # Look for row with typical header patterns
        header_keywords = ['date', 'time', 'value', 'temp', 'sensor', 'reading', 'timestamp']

        for idx, row in df.iterrows():
            row_str = ' '.join(row.astype(str).str.lower())
            if any(keyword in row_str for keyword in header_keywords):
                return idx

        # If no keywords found, assume first row with mostly non-numeric data
        for idx, row in df.iterrows():
            non_numeric = sum(pd.to_numeric(row, errors='coerce').isna())
            if non_numeric > len(row) / 2:
                return idx

        return 0  # Default to first row

    except:
        return 0


def parse_file_with_header(file_path, header_row=0, num_rows=10):
    """Parse file starting from the header row."""
    try:
        if str(file_path).endswith('.csv'):
            # Try different delimiters
            for delimiter in [',', '\t', ';', '|']:
                try:
                    df = pd.read_csv(
                        file_path,
                        sep=delimiter,
                        header=None,
                        skiprows=header_row,
                        nrows=num_rows,
                        encoding='utf-8',
                        encoding_errors='ignore',
                        on_bad_lines='skip'
                    )
                    if len(df.columns) > 1:  # Found a delimiter that splits the data
                        return df
                except:
                    continue

            # If no delimiter worked, try auto-detect
            df = pd.read_csv(
                file_path,
                sep=None,
                engine='python',
                header=None,
                skiprows=header_row,
                nrows=num_rows,
                encoding='utf-8',
                encoding_errors='ignore',
                on_bad_lines='skip'
            )
            return df
        else:
            df = pd.read_excel(file_path, header=None, skiprows=header_row, nrows=num_rows)
            return df
    except:
        return None


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Data Processing - V5")
    st.markdown("**Smart Configuration**: Auto-detect column types with manual override")
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

        # ========== STEP 2: CONFIGURE COLUMNS ==========
        st.header("Step 2: Configure Column Mappings")

        # Show tip and apply to all button at the top
        col_tip, col_apply = st.columns([3, 1])
        with col_tip:
            st.info("💡 Column types are auto-detected. Review and adjust dropdowns as needed.")

        with col_apply:
            if len(st.session_state.uploaded_files) > 1:
                # Check if we have at least one configured file
                has_config = len(st.session_state.column_mappings) > 0

                if st.button("🔄 Copy First File Settings ↓",
                           type="secondary",
                           use_container_width=True,
                           disabled=not has_config,
                           help="Configure the first file, then click to copy its settings to all others"):
                    if st.session_state.column_mappings:
                        # Get first file's configuration
                        first_file_name = list(st.session_state.uploaded_files.keys())[0]
                        if first_file_name in st.session_state.column_mappings:
                            first_config = st.session_state.column_mappings[first_file_name]

                            # Apply to all other files
                            for fname in st.session_state.uploaded_files.keys():
                                if fname != first_file_name:
                                    st.session_state.column_mappings[fname] = {
                                        'header_row': first_config['header_row'],
                                        'sensor_name': Path(fname).stem,  # Keep unique sensor names
                                        'columns': first_config['columns'].copy()
                                    }

                            st.success(f"✅ Settings from {first_file_name} copied to all files!")
                            st.rerun()
                        else:
                            st.warning("⚠️ Please configure the first file before copying settings.")

        # Configuration for all files on single page
        for idx, (file_name, file_path) in enumerate(st.session_state.uploaded_files.items()):
            with st.container():
                # File section with border
                if idx > 0:
                    st.markdown("---")

                # Compact header with file name and AI detection button
                col_file, col_ai = st.columns([3, 1])

                with col_file:
                    st.markdown(f"### 📄 {file_name}")

                with col_ai:
                    # AI auto-detect button
                    if st.button("🤖 Auto-Detect", key=f"ai_{file_name}", type="secondary", use_container_width=True):
                        # Read raw text for AI analysis
                        raw_text = read_raw_text(file_path, num_lines=15)

                        # Get AI detection
                        with st.spinner("Analyzing with AI..."):
                            ai_config = detect_columns_with_ai(raw_text, file_name)

                        if ai_config:
                            # Store AI configuration in session state
                            if 'ai_configs' not in st.session_state:
                                st.session_state.ai_configs = {}
                            st.session_state.ai_configs[file_name] = ai_config
                            st.success("✅ AI detection complete!")
                            st.rerun()

                # Check if we have AI configuration for this file
                ai_config = st.session_state.get('ai_configs', {}).get(file_name, None)

                # Key settings row
                col_header, col_sensor = st.columns([1, 2])

                # Detect or use AI-suggested header row
                if ai_config:
                    default_header_row = ai_config.get('skip_rows', 0)
                    default_sensor_name = ai_config.get('sensor_name', Path(file_name).stem)
                else:
                    default_header_row = detect_header_row(file_path)
                    default_sensor_name = Path(file_name).stem

                with col_header:
                    header_row = st.number_input(
                        "Header row",
                        min_value=0,
                        max_value=10,
                        value=default_header_row,
                        key=f"header_{file_name}",
                        help="Which row contains the column names?"
                    )

                with col_sensor:
                    sensor_name = st.text_input(
                        "Sensor name",
                        value=default_sensor_name,
                        key=f"sensor_{file_name}"
                    )

                # Parse and show preview
                df_preview = parse_file_with_header(file_path, header_row, num_rows=6)

                if df_preview is not None and not df_preview.empty:
                    # Create column configuration interface
                    column_types = ["Ignore", "Date", "Excel Time", "Value", "Notes"]

                    # Auto-detect column types or use AI configuration
                    auto_detected = {}

                    if ai_config:
                        # Use AI-detected configuration
                        for col_idx in range(len(df_preview.columns)):
                            if col_idx == ai_config.get('date_column', -1):
                                auto_detected[col_idx] = "Date"
                            elif col_idx == ai_config.get('excel_time_column', -1):
                                auto_detected[col_idx] = "Excel Time"
                            elif col_idx == ai_config.get('value_column', -1):
                                auto_detected[col_idx] = "Value"
                            else:
                                # Fallback to auto-detection for other columns
                                col_data = df_preview.iloc[:, col_idx]
                                header_name = df_preview.iloc[0, col_idx] if len(df_preview) > 0 else None
                                detected_type = auto_detect_column_type(col_data, header_name)
                                if detected_type == "Notes":
                                    auto_detected[col_idx] = "Notes"
                                else:
                                    auto_detected[col_idx] = "Ignore"
                    else:
                        # Use existing auto-detection
                        for col_idx in range(len(df_preview.columns)):
                            col_data = df_preview.iloc[:, col_idx]
                            # Get the header name from first row if available
                            header_name = df_preview.iloc[0, col_idx] if len(df_preview) > 0 else None
                            auto_detected[col_idx] = auto_detect_column_type(col_data, header_name)

                    # Create dropdowns for each column
                    st.markdown("**Column Assignments:**")
                    col_configs = st.columns(min(len(df_preview.columns), 8))  # Max 8 columns to prevent too narrow
                    column_mappings = {}

                    for col_idx in range(len(df_preview.columns)):
                        col = col_configs[col_idx % len(col_configs)]  # Wrap if more than 8 columns
                        with col:
                            # Show column header if available
                            if len(df_preview) > 0:
                                header_text = str(df_preview.iloc[0, col_idx])[:12]
                                st.caption(f"**Col {col_idx}:** {header_text}")

                            # Dropdown for column type
                            selected_type = st.selectbox(
                                f"Type",
                                options=column_types,
                                index=column_types.index(auto_detected[col_idx]),
                                key=f"col_{file_name}_{col_idx}",
                                label_visibility="collapsed"
                            )
                            column_mappings[col_idx] = selected_type

                            # Show preview of first few values
                            preview_vals = df_preview.iloc[1:3, col_idx].astype(str)
                            for val in preview_vals:
                                if len(val) > 15:
                                    st.caption(f"{val[:15]}...")
                                else:
                                    st.caption(val)

                    # Store configuration
                    if file_name not in st.session_state.column_mappings:
                        st.session_state.column_mappings[file_name] = {}

                    st.session_state.column_mappings[file_name] = {
                        'header_row': header_row,
                        'sensor_name': sensor_name,
                        'columns': column_mappings
                    }

                    # Show compact preview table
                    with st.expander("📊 View full data preview", expanded=False):
                        st.dataframe(df_preview, use_container_width=True, height=200)
                else:
                    st.error("Could not parse file. Please check the header row setting.")

        # ========== AI DEBUG WINDOW ==========
        # Always show debug window for testing purposes
        st.markdown("---")
        with st.expander("🔍 AI Debug Window - View API Requests & Responses", expanded=True):
            st.markdown("### AI Detection Log")
            st.caption(f"Total API calls: {len(st.session_state.ai_debug_log)}")

            # Add clear button
            if len(st.session_state.ai_debug_log) > 0:
                if st.button("🗑️ Clear Debug Log", key="clear_debug"):
                    st.session_state.ai_debug_log = []
                    st.rerun()
            else:
                st.info("No AI API calls yet. Click 'Auto-Detect' on a file to see debug information.")

            # Display each debug entry
            for idx, entry in enumerate(reversed(st.session_state.ai_debug_log)):
                with st.container():
                    # Header with file info and status
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"**#{len(st.session_state.ai_debug_log) - idx}: {entry['file_name']}**")
                    with col2:
                        st.caption(f"⏰ {entry['timestamp']}")
                    with col3:
                        if entry['success']:
                            st.success("✅ Success")
                        else:
                            st.error("❌ Failed")

                    # Show request details
                    with st.expander(f"📤 Request Details", expanded=False):
                        if entry['request']:
                            st.json({
                                'model': entry['request']['model'],
                                'max_tokens': entry['request']['max_tokens'],
                                'temperature': entry['request']['temperature'],
                                'prompt_length': entry['request']['prompt_length']
                            })

                            st.markdown("**Prompt Preview (first 500 chars):**")
                            st.code(entry['request']['prompt_preview'], language='text')

                            st.markdown("**Raw Text Sample Sent (first 5 lines):**")
                            for line_idx, line in enumerate(entry['request']['raw_text_lines']):
                                st.text(f"Line {line_idx}: {line[:100]}")

                    # Show response details
                    with st.expander(f"📥 Response Details", expanded=False):
                        if entry['response']:
                            st.markdown(f"**Response Length:** {entry['response']['response_length']} chars")

                            st.markdown("**Raw AI Response:**")
                            st.code(entry['response']['raw_text'], language='json')

                            if 'parsed_json' in entry['response']:
                                st.markdown("**Parsed Configuration:**")
                                st.json(entry['response']['parsed_json'])

                    # Show errors if any
                    if entry['error']:
                        st.error(f"**Error:** {entry['error']}")

                    if idx < len(st.session_state.ai_debug_log) - 1:
                        st.divider()

        # ========== STEP 3: COMBINE DATA ==========
        if st.session_state.column_mappings and len(st.session_state.column_mappings) == len(st.session_state.uploaded_files):
            st.markdown("---")
            st.header("Step 3: Combine All Data")

            # Show configuration summary
            with st.expander("📋 View Configuration Summary"):
                for file_name, config in st.session_state.column_mappings.items():
                    st.markdown(f"**{file_name}**")
                    col_summary = []
                    for col_idx, col_type in config['columns'].items():
                        if col_type != "Ignore":
                            col_summary.append(f"Column {col_idx}: {col_type}")
                    st.markdown(" | ".join(col_summary))

            if st.button("🔗 Combine All Files", type="primary", use_container_width=True):
                with st.spinner("Combining all files..."):
                    progress = st.progress(0)

                    try:
                        loaded_dfs = []
                        total_files = len(st.session_state.column_mappings)

                        for idx, (file_name, config) in enumerate(st.session_state.column_mappings.items()):
                            progress.progress((idx + 1) / total_files)

                            file_path = st.session_state.uploaded_files[file_name]

                            # Read full file
                            if file_path.endswith('.csv'):
                                df_full = pd.read_csv(
                                    file_path,
                                    header=None,
                                    skiprows=config['header_row'],
                                    encoding='utf-8',
                                    encoding_errors='ignore',
                                    on_bad_lines='skip'
                                )
                            else:
                                df_full = pd.read_excel(
                                    file_path,
                                    header=None,
                                    skiprows=config['header_row']
                                )

                            # Extract mapped columns
                            df_clean = pd.DataFrame()

                            for col_idx, col_type in config['columns'].items():
                                if col_type == "Date" and col_idx < len(df_full.columns):
                                    df_clean['Date'] = df_full.iloc[:, col_idx]
                                elif col_type == "Excel Time" and col_idx < len(df_full.columns):
                                    df_clean['Excel Time'] = df_full.iloc[:, col_idx]
                                elif col_type == "Value" and col_idx < len(df_full.columns):
                                    df_clean[config['sensor_name']] = df_full.iloc[:, col_idx]
                                elif col_type == "Notes" and col_idx < len(df_full.columns):
                                    df_clean['Notes'] = df_full.iloc[:, col_idx]

                            # Parse dates
                            if 'Date' in df_clean.columns:
                                df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='mixed', errors='coerce')
                                df_clean = df_clean.dropna(subset=['Date'])

                                loaded_dfs.append(df_clean)

                        # Combine all dataframes
                        if loaded_dfs:
                            combined = loaded_dfs[0].copy()
                            for df in loaded_dfs[1:]:
                                merge_cols = ['Date']
                                if 'Excel Time' in combined.columns and 'Excel Time' in df.columns:
                                    combined = pd.merge(combined, df, on=merge_cols, how='outer', suffixes=('', '_dup'))
                                    # Handle duplicate Excel Time columns
                                    for col in combined.columns:
                                        if col.endswith('_dup'):
                                            base_col = col[:-4]
                                            if base_col in combined.columns:
                                                combined[base_col] = combined[base_col].fillna(combined[col])
                                            combined = combined.drop(columns=[col])
                                else:
                                    combined = pd.merge(combined, df, on=merge_cols, how='outer')

                            # Sort and clean
                            combined = combined.sort_values('Date').reset_index(drop=True)

                            # Reorder columns
                            cols = ['Date']
                            if 'Excel Time' in combined.columns:
                                cols.append('Excel Time')
                            sensor_cols = [c for c in combined.columns if c not in ['Date', 'Excel Time', 'Notes']]
                            if 'Notes' in combined.columns:
                                sensor_cols.append('Notes')
                            combined = combined[cols + sensor_cols]

                            # Remove timezone if present
                            if pd.api.types.is_datetime64tz_dtype(combined['Date']):
                                combined['Date'] = combined['Date'].dt.tz_localize(None)

                            st.session_state.combined_df = combined

                            st.success(f"✅ Successfully combined {total_files} files!")
                            st.balloons()

                            # Show summary metrics
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Total Rows", f"{len(combined):,}")
                            with col2:
                                num_sensors = len([c for c in combined.columns if c not in ['Date', 'Excel Time', 'Notes']])
                                st.metric("Sensors", num_sensors)
                            with col3:
                                date_range = f"{combined['Date'].min().strftime('%m/%d/%Y')} - {combined['Date'].max().strftime('%m/%d/%Y')}"
                                st.metric("Date Range", date_range)
                            with col4:
                                st.metric("Total Columns", len(combined.columns))

                            # Show preview
                            st.markdown("### Data Preview")
                            st.dataframe(combined.head(20), use_container_width=True, height=400)

                    except Exception as e:
                        st.error(f"❌ Error combining files: {str(e)}")
                        st.exception(e)

        # ========== STEP 4: EXPORT ==========
        if st.session_state.combined_df is not None:
            st.markdown("---")
            st.header("Step 4: Export Combined Data")

            combined = st.session_state.combined_df

            col1, col2 = st.columns([2, 1])
            with col1:
                output_filename = st.text_input(
                    "Output filename",
                    value=f"combined_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                )

            with col2:
                st.markdown("###")  # Spacing
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

            # Additional export options
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


if __name__ == "__main__":
    main()