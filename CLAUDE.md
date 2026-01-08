# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fischer Data App V1 is a Streamlit-based data processing application for combining building management system sensor data from multiple Excel/CSV files. The app performs time-series alignment, timestamp normalization, quarter-hour resampling, and data quality flagging.

**LATEST (V12)**: Multi-column CSV support with checkbox interface for selecting multiple value columns (matching Excel workflow). AI auto-detects multi-column CSVs and returns array format. Column naming uses "{Filename} {ColumnName}" prefix for uniqueness. All V11 automatic workflow features preserved.

## Common Commands

### Running the Application
```bash
# Main application (latest version - V12 with multi-column CSV support)
streamlit run src/app_v12.py

# Alternative versions
streamlit run src/app_v11.py      # Version 11 with automatic workflow (single-column CSV only)
streamlit run src/app_v10.py      # Version 10 with tab-based UI and smart data types
streamlit run src/app_v9.py       # Version 9 with multi-tab Excel and enhanced flagging
streamlit run src/app_v7.py       # Version 7 with quarter-hour resampling
streamlit run src/app_v6.py       # Version 6 with parallel AI and timestamp normalization
streamlit run src/app_v5.py       # Version 5 with per-file AI detection
streamlit run src/app_v4.py       # Version 4 with raw text preview
streamlit run src/app.py          # Original tab-based version
streamlit run src/app_simple.py   # Simplified version (no resampling)
```

### Installation
```bash
pip install -r requirements.txt
```

## Architecture & Key Components

### Core Data Processing Module
**`src/data_processor.py`** - Contains the `DataProcessor` class for all data operations:
- **File Loading**: Flexible parsing with automatic header detection
- **Data Merging**: Outer joins on timestamps for combining multiple sensor files
- **No Excel Time Column**: Excel Time support removed (was a data artifact)
- **Export**: Generates clean combined CSV files

**IMPORTANT CHANGES IN V6**:
- Excel Time column handling completely removed
- Simplified to Date + Value columns only
- All merge operations use Date column exclusively

### Timestamp Normalization Module (NEW)
**`src/timestamp_normalizer.py`** - Robust timestamp parsing and standardization:
- **Input Formats Supported**:
  - US-style: `7/18/2024 12:45:00 PM EDT`
  - 24-hour: `07/18/2024 14:30:00`
  - Text month: `July 18, 2024 2:30 PM`
  - ISO-style: `2024-07-18 14:30:00`
  - With/without seconds
  - With/without timezone abbreviations (EST, EDT, CST, etc.)
- **Output Format**: Always `MM/DD/YYYY HH:MM:SS` (24-hour)
- **Timezone Handling**: Maps abbreviations to IANA zones, defaults to America/New_York
- **Key Functions**:
  - `normalize_timestamp()`: Parse and normalize any timestamp
  - `format_timestamp_mdy_hms()`: Return standardized MM/DD/YYYY HH:MM:SS
  - `detect_timestamp_format()`: Human-readable format description for preview

### UI Application Versions
The project has evolved through multiple UI iterations in `src/`:

- **`app_v12.py`** (CURRENT - RECOMMENDED):
  - **All V11 Features**: Automatic workflow, progress tracking, file archiving, tab-based UI, smart data types
  - **Multi-Column CSV Support** (NEW):
    - AI auto-detects multiple value columns in CSV files
    - Returns `value_columns` array instead of single `value_column`
    - Checkbox interface for selecting which columns to include (matches Excel workflow)
    - Column naming: `"{Filename} {ColumnName}"` for uniqueness across files
  - **Updated AI Prompt** (NEW):
    - CSV prompt now asks for array of value columns
    - Response format: `{"value_columns": [2, 3, 4], "column_names": ["Temp", "Humidity", "Fan"]}`
    - Backward compatible: auto-converts old single-column format
  - **Updated Config Structure** (NEW):
    - CSV config now uses `available_columns`, `column_names`, `selected_columns` arrays
    - Matches multi-tab Excel config structure for consistency
  - **Updated UI**:
    - `render_csv_config_ui()` now shows checkbox grid (3 columns) for column selection
    - Preview shows all selected columns with filename prefix
    - Fallback for manual column addition if AI detection fails
  - **Updated Data Extraction**:
    - `auto_process_and_export()` loops through `selected_columns` array
    - Each column named: `"{Filename} {ColumnName}"`
    - Smart type conversion applied per column
  - **Memory Optimization** (CRITICAL FIX - Jan 2026):
    - Replaced nested dictionary `inexact_cells` with Boolean DataFrame
    - ~100x memory reduction for wide datasets (1000+ sensors)
    - Prevents "blank out" memory crashes on 4GB RAM systems
    - See "Bug Fix #4" in V12 Bug Fixes section for details
  - Uses `.env` file for CLAUDE_API_KEY

- **`app_v11.py`**:
  - **All V10 Features**: Tab-based UI, smart data types, enhanced flagging, multi-tab Excel, Fischer branding
  - **Fully Automatic Workflow**:
    - Single "Process All Files" button triggers entire pipeline
    - Automatic: Combine → Save Raw CSV → Resample → Generate Excel
    - All files automatically saved to archive folder
    - No manual steps after clicking process button
  - **Progress Tracking** (NEW):
    - Real-time progress bar (0-100%) across all phases
    - Phase weights: Combine (40%), Resample (40%), Export (20%)
    - Status messages show current operation
    - Progress callback updates every 100 rows during resampling
  - **Automatic File Generation** (NEW):
    - Raw CSV: `{Building}_raw_merged_{timestamp}.csv` → archive folder
    - Excel: `{Building}_resampled_15min_{timestamp}.xlsx` → archive folder
    - Files created without user intervention
    - Sanitized building names for filenames
  - **Streamlined 4-Step Process** (NEW):
    - Step 1: Upload & Archive
    - Step 2: AI Analysis
    - Step 3: Review & Edit Configurations
    - Step 4: Process & Export (automatic)
  - **Removed AI Debug Panel**: Cleaner interface, no debug section
  - **New Helper Functions**:
    - `auto_process_and_export()`: 200+ line orchestration function
    - `sanitize_building_name()`: Clean filenames from building names
    - `validate_archive_path()`: Ensure archive directory exists and is writable
  - **Download Interface** (NEW):
    - Two download buttons appear after processing
    - Shows file paths where files were saved
    - Preview tabs: Resampled Data + Quality Statistics
    - Reset button to process different files
  - **One-Shot Processing**: No option to re-run individual steps after completion
  - Uses `.env` file for CLAUDE_API_KEY

- **`app_v10.py`**:
  - **All V9 Features**: Multi-tab Excel, enhanced flagging, file archiving, Fischer branding
  - **Tab-Based UI Redesign** (NEW):
    - Two-level tab system: File-level tabs → Sheet-level tabs (for multi-tab Excel)
    - Visual indicators on tabs: `✓ (count)`, `⚠️`, `❌`
    - Replaces accordion interface (no more scrolling through 40 open sections)
    - Better UX: Click tabs to configure each file/sheet
  - **Smart Data Type Preservation** (NEW):
    - Intelligent column type detection with 80% threshold
    - Pure text columns ("off"/"on") preserved as text
    - Mostly numeric columns (72.5, -, 73.1) converted to numeric
    - SQL-ready output with proper data types (INTEGER, FLOAT, VARCHAR)
    - Uses `dtype=str` + `keep_default_na=False` on all reads, then `smart_convert_column()`
  - **Improved Quality Flagging** (NEW):
    - Stale data detection: Only checks numeric columns (skips text fields)
    - Zero value detection: Only checks numeric columns
    - Prevents false positives on text fields like fan status
  - **Helper Functions Added**:
    - `build_tab_label()`: Dynamic tab label generation with visual indicators
    - `smart_convert_column()`: Intelligent type conversion (text vs numeric)
    - `render_sheet_config_ui()`: Excel sheet configuration UI
    - `render_csv_config_ui()`: CSV/single-tab file configuration UI
  - **6-Step Workflow**: Upload & Archive → AI Analysis → Review (Tabs!) → Combine → Resample → Export
  - Uses `.env` file for CLAUDE_API_KEY

- **`app_v9.py`**:
  - **All V7 Features**: Quarter-hour resampling, quality flags, two-stage export, parallel AI
  - **Multi-Tab Excel Support**: Process Excel files with multiple worksheets
    - AI-powered tab detection and column identification
    - Dynamic column naming: `[TabName] [ColumnName]`
    - Per-tab column selection UI
    - Preserves percentage formatting where possible
  - **Enhanced Quality Flags**:
    - `Stale_Data_Flag`: Modified to detect 3+ consecutive non-zero identical values (changed from 4+ in V7)
    - `Stale_Sensors`: Comma-separated list of sensors with stale readings
    - `Zero_Value_Flag`: New column with values "Clear", "Single", "Repeated" for zero tracking
  - **File Archiving System**:
    - Building Name input field for organization
    - Default archive structure: `archive/[Building Name]/`
    - Optional custom archive location via UI checkbox
    - Automatic copying of original files before processing
  - **Fischer Energy Branding**:
    - Custom logo in top-left (fischer background clear)
    - Teal color scheme (#24b3aa primary)
    - Custom CSS injection for buttons and headers
    - Configured via `.streamlit/config.toml`
  - **Improved Column Ordering**: Date, Stale_Data_Flag, Stale_Sensors, Zero_Value_Flag, then all sensors
  - **6-Step Workflow**: Upload & Archive → AI Analysis → Review → Combine → Resample → Export
  - Uses `.env` file for CLAUDE_API_KEY

- **`app_v7.py`**:
  - **All V6 Features**: Parallel AI processing, timestamp normalization, simplified configuration
  - **Two-Stage Export**: Raw merged CSV download + resampled CSV export
  - **Quarter-Hour Resampling**: Automatic resampling to 15-minute intervals (:00, :15, :30, :45)
  - **Quality Flags**:
    - `Inexact_Match_Flag`: Identifies timestamps not exactly on 15-min marks
    - `{Sensor}_Stale_Flag`: Detects 4+ consecutive identical readings per sensor
  - **merge_asof Algorithm**: Efficient nearest-value matching within ±2 minute tolerance
  - **Resampling Statistics**: Detailed metrics on data quality and completeness
  - **6-Step Workflow**: Upload → AI Analysis → Review → Combine → Resample → Export
  - Uses `.env` file for CLAUDE_API_KEY

- **`app_v6.py`**: Streamlined with parallel AI analysis and timestamp normalization
  - **Parallel AI Processing**: Analyzes all uploaded files simultaneously using ThreadPoolExecutor (5 workers)
  - **Simplified Configuration**: Single `start_row` parameter (removed confusing `skip_rows` + `header_row`)
  - **No Excel Time Column**: Removed all Excel Time handling (was a data artifact)
  - **Timestamp Normalization**: Automatic conversion to MM/DD/YYYY HH:MM:SS with inline preview
  - **Visual Feedback**: Shows extracted data preview immediately with timestamp conversion example
  - **Batch Analysis**: One-click "Analyze All Files" button with progress tracking
  - **Enhanced Debug Panel**: Shows all API requests/responses for all files

- **`app_v5.py`**: AI-powered per-file column detection with debug window (sequential processing)
- **`app_v4.py`**: Raw text preview first, then user-guided configuration
- **`app.py`**: Original tab-based interface
- **`app_v2.py`**: Enhanced column selection workflow
- **`app_v3.py`**: Simplified configuration
- **`app_simple.py`**: No resampling/flagging, just combines data

### Data Processing Pipeline

**V9 (Current):**
```
Raw Files → File Type Detection (CSV/Single-Tab/Multi-Tab) → AI Analysis (parallel) →
User Review/Edit → [Multi-Tab: Column Selection] → Archive Original Files →
Combine (timestamp normalization) → [Optional: Download Raw Merged CSV] →
Resample to 15-min → Enhanced Quality Flagging (Stale + Zero) → Column Reordering → Export CSV
```

**V7:**
```
Raw Files → AI Analysis (parallel) → User Review/Edit → Combine (timestamp normalization) →
[Optional: Download Raw Merged CSV] → Resample to 15-min → Quality Flagging → Export Resampled CSV
```

**V6:**
```
Raw Files → AI Analysis (parallel) → User Review/Edit → Combine (timestamp normalization) → Export CSV
```

Key implementation details:
- Handles multiple date formats via robust timestamp normalization
- All timestamps standardized to MM/DD/YYYY HH:MM:SS format
- Excel Time column support removed in V6 (was a data artifact)
- Only Date and Value columns extracted from each file
- Outer join on Date column for combining multiple sensors
- V7 adds quarter-hour resampling with `pd.merge_asof()` for efficient nearest-value matching

### Session State Management
Streamlit apps use session state to maintain:
- `uploaded_files`: Dictionary of file names to file paths
- `file_configs`: Dictionary of file configurations
  - **V9 CSV/Single-Tab**: `{'file_type': 'csv', 'config': {start_row, delimiter, date_column, value_column, sensor_name}}`
  - **V9 Multi-Tab**: `{'file_type': 'excel_multi_tab', 'tabs': {tab_name: {start_row, date_column, selected_columns, ...}}}`
- `combined_df`: Raw merged DataFrame (all sensors combined)
- `resampled_df`: 15-minute resampled DataFrame with quality flags (V7+)
- `resampling_stats`: Dictionary with resampling metrics (V7+)
- `building_name`: Building name for archiving (V9 only)
- `archive_path`: Archive folder path (V9 only)
- `use_custom_archive`: Boolean for custom archive location (V9 only)
- `ai_debug_log`: List of AI API call details (V6+)
- `ai_analysis_complete`: Boolean flag for batch analysis status (V6+)

## File Structure & Patterns

### Input Data Format
Excel/CSV files in `CSVdata/` with varying structures:
- May have metadata header rows before column names
- Column names row contains: Date, Value, Notes (Excel Time no longer used)
- Data rows with timestamps and sensor values
- Timestamps can be in various formats (handled by normalizer)

### Output Format
CSV files in `output/` containing:

**V9 Structure:**
- `Date` column (standardized to MM/DD/YYYY HH:MM:SS)
- `Stale_Data_Flag` (Boolean: TRUE/FALSE)
- `Stale_Sensors` (Text: comma-separated list or blank)
- `Zero_Value_Flag` (Text: "Clear", "Single", or "Repeated")
- Multiple sensor value columns (alphabetically sorted)
- All timestamps aligned on Date
- Quarter-hour intervals (:00, :15, :30, :45)

**Earlier Versions:**
- Date + sensor columns only (V6 and earlier)
- Date + Inexact_Match_Flag + per-sensor stale flags + sensors (V7)

### Key Code Patterns

**Flexible file reading:**
```python
df = pd.read_csv(file_path, sep=delimiter, header=start_row,
                 encoding='utf-8', encoding_errors='ignore')
```

**Timestamp normalization:**
```python
from timestamp_normalizer import format_timestamp_mdy_hms

# Normalize any timestamp to MM/DD/YYYY HH:MM:SS
normalized = format_timestamp_mdy_hms(original_timestamp_string)
```

**Parallel AI processing:**
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(analyze_file, f): f for f in files}
    for future in as_completed(futures):
        result = future.result()
```

## AI Integration (app_v6.py)

### Claude AI Auto-Detection (V6)
The current version uses Claude AI for parallel batch analysis:

**Key Function**: `analyze_all_files_parallel()` in app_v6.py
- Analyzes first 15 lines of raw file content
- Processes all files simultaneously using ThreadPoolExecutor
- Returns structured JSON with column mappings
- Model: Claude Sonnet 4.5 (claude-sonnet-4-5-20250929)
- Typical response time: 1-3 seconds per file (parallel)

**What AI Detects (V6)**:
- `start_row`: Row index where column headers are located (0-based)
- `delimiter`: Column separator (comma, tab, semicolon, pipe)
- `date_column`: Column index for date/timestamp (0-based)
- `value_column`: Column index for sensor value (0-based)
- `sensor_name`: Extracted from metadata or suggested from filename

**JSON Response Format (V6)**:
```json
{
  "delimiter": ",",
  "start_row": 1,
  "date_column": 0,
  "value_column": 2,
  "sensor_name": "CH-2 CHWS Temp"
}
```

**REMOVED IN V6**:
- `skip_rows` (replaced by single `start_row`)
- `header_row` (merged into `start_row`)
- `excel_time_column` (Excel Time no longer used)

### AI Debug Panel (V6)
Unified debug console at bottom of app showing all parallel analysis:
- Total API calls and success rate metrics
- Individual file analysis details in expandable sections
- Request details: model, tokens, prompt preview, raw text sample
- Response details: raw AI output and parsed JSON
- Error messages for failed analyses
- Success/failure status for each file
- Clear log button to reset debug information

**Debug Log Structure (V6)**:
```python
st.session_state.ai_debug_log = [
    {
        'file_name': str,
        'timestamp': str,  # HH:MM:SS format
        'request': {
            'model': str,
            'max_tokens': int,
            'temperature': float,
            'prompt_length': int,
            'prompt_preview': str,
            'raw_text_lines': list  # First 5 lines
        },
        'response': {
            'raw_text': str,
            'response_length': int,
            'parsed_json': dict  # If successful
        },
        'error': str | None,
        'success': bool
    }
]
```

### Environment Setup
AI integration requires:
- `.env` file in project root with `CLAUDE_API_KEY=sk-ant-...`
- `anthropic>=0.71.0` package (in requirements.txt)
- API key with access to Claude Sonnet 4.5 models

### Excel File Handling
For Excel files, `read_raw_lines()` converts to CSV-like format:
- Reads with `pd.read_excel(header=None, nrows=15)`
- Converts to CSV string via `io.StringIO()`
- Ensures AI receives readable text for analysis

## Quarter-Hour Resampling (V7)

### Algorithm Details
V7 implements efficient quarter-hour resampling using pandas' `merge_asof()`:

**Process:**
1. **Create Target Timestamps**: Generate complete 15-minute interval grid from data start to end
2. **Nearest-Value Matching**: Use `pd.merge_asof()` with `direction='nearest'` and `tolerance=±2min`
3. **Quality Flagging**: Automatically flag data quality issues

**Quality Flags:**

1. **Inexact_Match_Flag** (Boolean per row):
   - `True` when original timestamp minute is not 0, 15, 30, or 45
   - `True` when original timestamp has non-zero seconds
   - `True` when no data found within ±2 minute tolerance
   - Helps identify interpolated/estimated values

2. **{Sensor}_Stale_Flag** (Boolean per row, per sensor):
   - `True` when current value equals previous 3 values (4 consecutive identical)
   - Indicates potentially stuck/malfunctioning sensors
   - Checked separately for each sensor column
   - `NaN` values are not flagged as stale

**Resampling Function:**
```python
def resample_to_quarter_hour(combined_df, tolerance_minutes=2):
    # 1. Create 15-min timestamp grid
    target_timestamps = pd.date_range(start, end, freq='15min')

    # 2. Use merge_asof for efficient nearest matching
    resampled = pd.merge_asof(
        target_df, combined_df,
        left_on='Date_Target', right_on='Date',
        direction='nearest', tolerance=pd.Timedelta(minutes=2)
    )

    # 3. Flag inexact matches
    Inexact_Match_Flag = (minute % 15 != 0) | (second != 0)

    # 4. Flag stale data (rolling window check)
    {Sensor}_Stale_Flag = (val == shift(1)) & (val == shift(2)) & (val == shift(3))

    return resampled, stats
```

**Statistics Provided:**
- Total 15-minute intervals created
- Count and percentage of inexact matches
- Stale data count per sensor
- Total stale data points across all sensors
- Date range coverage

### Deduplication Strategy
- V7 uses `drop_duplicates()` after outer merge to remove exact duplicate rows
- Outer merge naturally handles overlapping timestamps (creates single row with all sensor data)
- No additional deduplication needed for quarter-hour intervals

## V9 Enhanced Features

### Multi-Tab Excel File Support (V9)

V9 adds comprehensive support for Excel files with multiple worksheet tabs:

**File Type Detection:**
```python
def detect_file_type(file_path):
    """Returns: Tuple of (file_type, sheet_names)"""
    if file_path.endswith(('.xlsx', '.xls')):
        xl_file = pd.ExcelFile(file_path)
        if len(xl_file.sheet_names) > 1:
            return 'excel_multi_tab', xl_file.sheet_names
        else:
            return 'excel_single_tab', xl_file.sheet_names
    else:
        return 'csv', None
```

**Multi-Tab AI Analysis:**
- AI analyzes first 15 lines of each tab separately
- Detects column structure per tab
- Identifies multiple value columns per tab
- Returns nested JSON structure:

```json
{
  "tabs": [
    {
      "tab_name": "AC12-1",
      "start_row": 1,
      "date_column": 0,
      "value_columns": [2, 3, 4],
      "column_names": ["Return Air Temp", "Supply Air Temp", "Fan Status"]
    },
    {
      "tab_name": "AC12-2",
      "start_row": 1,
      "date_column": 0,
      "value_columns": [2, 3],
      "column_names": ["Return Air Temp", "Supply Air Temp"]
    }
  ]
}
```

**Column Selection UI:**
- Per-tab expandable sections in Step 3
- Checkboxes for each detected value column
- User can include/exclude columns as needed
- Date column selector per tab

**Dynamic Column Naming:**
- Format: `[Tab Name] [Column Name]`
- Examples:
  - `AC12-1 Return Air Temp`
  - `AC12-1 Supply Air Temp`
  - `AC12-2 Return Air Temp`
- Ensures uniqueness across tabs

**Data Extraction:**
```python
def extract_multi_tab_data(file_path, file_config):
    """Extract data from multi-tab Excel file."""
    all_dataframes = []

    for tab_name, tab_config in file_config['tabs'].items():
        df = pd.read_excel(file_path, sheet_name=tab_name,
                          header=tab_config['start_row'])

        for col_idx in tab_config['selected_columns']:
            final_name = f"{tab_name} {column_name}"
            mini_df = pd.DataFrame({
                'Date': date_series,
                final_name: df.iloc[:, col_idx]
            })
            all_dataframes.append(mini_df)

    return all_dataframes
```

**Config Structure:**
- CSV/Single-Tab: `{'file_type': 'csv', 'config': {...}}`
- Multi-Tab: `{'file_type': 'excel_multi_tab', 'tabs': {...}}`

### Enhanced Quality Flagging (V9)

V9 modifies stale flag logic and adds zero value tracking:

#### **Stale Data Flag (Modified in V9)**

**V9 Logic (3+ consecutive non-zero):**
```python
# Changed from 4+ consecutive to 3+ consecutive
# Now only flags non-zero values (zeros handled separately)

stale_per_sensor = {}
for sensor in sensor_cols:
    # Skip if value is zero or NaN
    is_non_zero = (resampled[sensor] != 0) & (resampled[sensor].notna())

    # Check if current equals previous 2 values (3 consecutive identical non-zero)
    is_stale = (
        is_non_zero &
        (resampled[sensor] == resampled[sensor].shift(1)) &
        (resampled[sensor] == resampled[sensor].shift(2))
    )
    stale_per_sensor[sensor] = is_stale
```

**Output Columns:**
- `Stale_Data_Flag` (Boolean): TRUE if any sensor has stale data on this row
- `Stale_Sensors` (Text): Comma-separated list of sensor names with stale readings

**Example:**
```csv
Date,Stale_Data_Flag,Stale_Sensors,Sensor A,Sensor B,Sensor C
01/15/2024 00:00:00,FALSE,,72.5,55.3,45.8
01/15/2024 00:15:00,FALSE,,72.5,55.3,45.8
01/15/2024 00:30:00,TRUE,"Sensor A, Sensor B",72.5,55.3,46.2
```

#### **Zero Value Flag (New in V9)**

Tracks zero readings across all sensors with three states:

**Flag Values:**
- `"Clear"`: No zero values detected in any sensor
- `"Single"`: One or more sensors have isolated zero(s)
- `"Repeated"`: One or more sensors have 2+ consecutive zeros

**V9 Logic:**
```python
def calculate_zero_flags(resampled_df, sensor_cols):
    """Calculate Zero_Value_Flag for each row."""
    zero_flags = []

    for idx in range(len(resampled_df)):
        row_flag = "Clear"

        for sensor in sensor_cols:
            val_current = resampled_df.loc[idx, sensor]

            if pd.isna(val_current) or val_current != 0:
                continue

            # Current value is zero
            if idx > 0:
                val_prev = resampled_df.loc[idx-1, sensor]
                if val_prev == 0:
                    row_flag = "Repeated"  # Highest priority
                    break
                else:
                    if row_flag == "Clear":
                        row_flag = "Single"
            else:
                if row_flag == "Clear":
                    row_flag = "Single"

        zero_flags.append(row_flag)

    return zero_flags
```

**Priority:** Repeated > Single > Clear

**Example:**
```csv
Date,Zero_Value_Flag,Sensor A,Sensor B
01/15/2024 00:00:00,Clear,72.5,55.3
01/15/2024 00:15:00,Single,0,55.3
01/15/2024 00:30:00,Repeated,0,55.3
01/15/2024 00:45:00,Clear,72.5,55.3
01/15/2024 01:00:00,Single,72.5,0
```

### File Archiving System (V9)

V9 automatically archives all uploaded files before processing:

**Archive Structure:**
```
archive/
└── [Building Name]/
    ├── sensor_file_1.csv
    ├── sensor_file_2.xlsx
    └── sensor_file_3.csv
```

**Key Features:**
- Building Name input field (required)
- Default path: `archive/[Building Name]/`
- Optional custom location via UI checkbox
- Uses `shutil.copy2()` to preserve file metadata
- Archives created before any processing begins

**Implementation:**
```python
def archive_uploaded_files(uploaded_files, archive_path):
    """Copy original files to archive directory for safekeeping."""
    archive_dir = Path(archive_path)
    archive_dir.mkdir(parents=True, exist_ok=True)

    archived_files = []
    for file_name, file_path in uploaded_files.items():
        dest_path = archive_dir / file_name
        shutil.copy2(file_path, dest_path)
        archived_files.append(str(dest_path))

    return archived_files
```

**UI Options:**
- **Default Mode**: Auto-generates path from building name
- **Custom Mode**: Checkbox enables manual path entry
  - Examples: `C:/MyFiles/Archive`, `D:/Backups/BuildingData`
  - Full filesystem access for flexibility

### Fischer Energy Branding (V9)

**Color Scheme:**
- Primary Teal: `#24b3aa`
- Background White: `#FFFFFF`
- Text Black: `#151515`
- Secondary Gray: `#f0f2f6`

**Theme Configuration (`.streamlit/config.toml`):**
```toml
[theme]
primaryColor = "#24b3aa"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#f0f2f6"
textColor = "#151515"
font = "sans serif"
```

**Custom CSS Injection:**
```python
def inject_custom_css():
    """Inject custom CSS for Fischer Energy branding."""
    st.markdown("""
        <style>
        .stButton > button {
            background-color: #24b3aa;
            color: #FFFFFF;
        }
        h1, h2, h3 {
            color: #24b3aa;
        }
        </style>
        """, unsafe_allow_html=True)
```

**Logo Display:**
```python
def add_logo():
    """Add Fischer Energy logo to top-left with title."""
    logo_path = Path(__file__).parent.parent / "assets" / "fischer background clear (1).png"

    if logo_path.exists():
        with open(logo_path, "rb") as f:
            logo_data = base64.b64encode(f.read()).decode()

        st.markdown(
            f'<img src="data:image/png;base64,{logo_data}" style="height: 80px;">',
            unsafe_allow_html=True
        )
```

**Assets Required:**
- Logo file: `assets/fischer background clear (1).png`
- Transparent PNG recommended for clean display

## Performance Considerations

- Current: Handles ~12 files × 100 rows each efficiently
- Target: Designed for 10-90 files × 100-600,000 rows each
- Memory: ~54M rows fits in 2-4 GB RAM
- All pandas operations use optimized C implementations
- **V12 Memory Optimization** (CRITICAL):
  - Boolean DataFrame for inexact cell tracking instead of nested dictionaries
  - ~100x memory reduction for wide datasets (1000+ sensors)
  - Enables processing of 1034 sensors × 5000 rows on 4GB RAM systems
  - Vectorized operations replace per-cell loops for better performance
- **V9 AI Analysis**: ~1-3 seconds per file, runs 5 files in parallel (ThreadPoolExecutor)
  - Multi-tab files analyzed per-tab (slightly longer processing time)
- **Timestamp Normalization**: Adds minimal overhead (~0.1ms per timestamp)
- **V9 Resampling**: `merge_asof()` is O(n log n), efficient for large datasets
- **V9 Flag Operations**: Vectorized pandas operations, very fast
  - Zero flag calculation: O(n) per sensor per row
  - Stale flag calculation: Vectorized shift operations
- **V9 File Archiving**: Uses `shutil.copy2()`, minimal overhead
- **V9 Multi-Tab Extraction** (OPTIMIZED):
  - Creates 1 DataFrame per tab (not per column) - 75% fewer DataFrames
  - Uses `reduce()` for efficient merging
  - Enhanced progress indicators for large datasets
  - Typical performance: 8 files × 5 tabs × 4 columns = 40 DataFrames (was 160)

## V9 Bug Fixes (Production Issues)

### Bug Fix #1: Streamlit Deprecation Warnings (Fixed)
**Issue**: `use_container_width` parameter deprecated in Streamlit 1.31+

**Solution**: Removed all 12 instances of `use_container_width=True` from st.dataframe(), st.button(), and st.download_button() calls

**Files Modified**: `src/app_v9.py` (lines 1145, 1250, 1284, 1379, 1389, 1450, 1557, 1591, 1594, 1612, 1649, 1682, 1704)

### Bug Fix #2: PyArrow Serialization Errors (Fixed)
**Issue**: `ArrowTypeError: Expected bytes, got 'int'/'float' object` when displaying DataFrames with mixed data types

**Root Cause**: Multi-tab extraction created object columns with mixed int/float/None values, incompatible with PyArrow serialization

**Solutions Implemented**:
1. Added `pd.to_numeric(errors='coerce')` in `extract_multi_tab_data()` (line 857)
2. Created `prepare_df_for_display()` helper function for type enforcement (lines 799-828)
3. Wrapped 5 critical st.dataframe() calls with helper function

**Files Modified**: `src/app_v9.py`

### Bug Fix #3: Performance Hang with Large Multi-Tab Files (Fixed)
**Issue**: App hung when combining 8 multi-tab Excel files with many rows

**Root Cause**: Created 160+ separate DataFrames (one per column) with redundant timestamp normalization and iterative O(n²) merging

**Solutions Implemented**:
1. **Refactored `extract_multi_tab_data()`** (lines 831-881):
   - Now creates 1 DataFrame per tab instead of per column
   - Normalizes timestamps once per tab (not per column)
   - Result: 75% fewer DataFrames (160 → 40 for typical workload)

2. **Replaced iterative merge with `reduce()`** (lines 1565-1570):
   - Cleaner code with same functionality
   - Works with outer joins to preserve overlapping timestamps

3. **Enhanced progress indicators** (lines 1486-1495, 1580-1582, 1605-1607):
   - Shows current file being processed
   - Displays progress counter (e.g., "Processing file.xlsx... (3/8)")
   - Clears indicators after completion

**Performance Improvement**: ~50-70% faster for large multi-tab datasets, significantly reduced memory usage

**Files Modified**: `src/app_v9.py`

## V12 Bug Fixes (Production Issues)

### Bug Fix #4: Memory Crash with Wide Datasets (Fixed - January 2026)
**Issue**: Application would "blank out" (run out of memory) when processing wide datasets with 1000+ sensors on memory-constrained environments (e.g., Replit with 4GB RAM)

**Root Cause**: The `inexact_cells` tracking structure used a nested Python dictionary `{row_idx: {sensor_name: bool}}` which created millions of Python objects for large datasets. Example: 1034 sensors × 5000 rows = 5+ million dictionary entries causing ~100x memory overhead compared to numpy array storage.

**Solutions Implemented**:
1. **Replaced nested dictionary with Boolean DataFrame** (line 837):
   - Changed from: `inexact_cells = {i: {} for i in range(len(target_timestamps))}`
   - Changed to: `inexact_df = pd.DataFrame(index=range(len(target_timestamps)))`
   - Uses pandas/numpy backend for ~100x memory reduction

2. **Vectorized empty sensor handling** (line 852):
   - Removed per-row loop: `for row_idx in range(len(target_timestamps)): inexact_cells[row_idx][sensor] = False`
   - Replaced with: `inexact_df[sensor] = False` (entire column in one operation)

3. **Vectorized inexact cell tracking** (lines 919-920):
   - Removed per-row loop with dictionary assignment
   - Replaced with vectorized DataFrame column assignment: `inexact_df[sensor] = is_inexact.values`

4. **Updated `export_to_excel()` function** (lines 1507-1511):
   - Changed from dictionary lookup: `inexact_cells[row_idx].get(sensor, False)`
   - Changed to DataFrame access: `inexact_df.iat[data_row_idx, inexact_df.columns.get_loc(col_name)]`
   - Uses `.iat[]` for fast scalar access in tight loop

5. **Updated session state initialization** (lines 67, 790, 2331):
   - Changed from: `st.session_state.inexact_cells = {}`
   - Changed to: `st.session_state.inexact_cells = pd.DataFrame()`

**Memory Improvement**:
- **Before**: Millions of Python dict objects for 1034 sensors × thousands of rows = gigabytes of memory
- **After**: Single Boolean DataFrame using numpy array backend = ~100x less memory (megabytes)
- **Result**: Can now process 1000+ sensor datasets on 4GB RAM systems without crashing

**Performance Impact**: Neutral to positive - vectorized operations are often faster than loops

**Files Modified**: `src/app_v12.py` (lines 67, 790, 837, 852, 919-920, 1016, 1451-1511, 2331)

### Bug Fix #5: Excel Export Slowdown with Wide Datasets (Fixed - January 2026)
**Issue**: After fixing the memory crash (Bug Fix #4), Excel export became the bottleneck, taking minutes instead of seconds for datasets with 1000+ sensors × 5000 rows.

**Root Cause**: The `export_to_excel()` function used nested Python loops to write and style every cell individually. For 1000 sensors × 5000 rows = 5 million cell visits in slow Python loops instead of using optimized pandas/numpy operations.

**Solutions Implemented**:
1. **Bulk Write with pandas ExcelWriter** (lines 1478-1480):
   - Changed from: Manual cell-by-cell writing with nested loops
   - Changed to: `export_df.to_excel(writer, index=False, sheet_name='Resampled Data')`
   - Uses pandas' optimized C code for data writing

2. **Sparse Styling with np.where()** (lines 1496-1517):
   - Changed from: Checking every cell in nested loops (5M iterations)
   - Changed to: `np.where()` to find only cells needing color (~thousands)
   - Only visits cells that actually need yellow/red highlighting

3. **Sample-Based Column Width** (lines 1519-1527):
   - Changed from: Scanning all rows to calculate column widths
   - Changed to: Check header + first 10 data rows only
   - Provides reasonable widths with minimal overhead

4. **Added CSV Fallback** (lines 1532-1538):
   - If Excel export fails, automatically saves as CSV
   - Ensures data is not lost even if styling fails

5. **Added numpy import** (line 21):
   - Required for `np.where()` vectorized operations

**Performance Improvement**:
- **Before**: 5,000,000 Python loop iterations for cell writing + styling
- **After**: 1 bulk write + ~10,000 sparse styling operations (only "bad" cells)
- **Expected time**: Reduced from minutes to seconds

**Loop Iterations Comparison**:
| Operation | Before | After |
|-----------|--------|-------|
| Data writing | 5M cell-by-cell | 1 bulk operation |
| Cell styling | Check all 5M cells | Visit ~10K flagged cells |
| Column width | Scan all 5M cells | Sample 10 rows/column |

**Memory Impact**: Same as before (no additional memory usage)

**Files Modified**: `src/app_v12.py` (lines 21, 36, 1448-1538)

## Dependencies

### Required Packages (requirements.txt)
```
pandas>=2.0.0           # Data processing
numpy>=1.24.0           # Array operations (used directly for sparse styling)
openpyxl>=3.1.0         # Excel file support
streamlit>=1.28.0       # Web UI framework
plotly>=5.17.0          # Visualization (optional)
anthropic>=0.18.0       # Claude AI API
python-dotenv>=1.0.0    # Environment variable management
python-dateutil>=2.8.2  # Timestamp parsing (dep of pandas)
tzdata>=2022.1          # Timezone database
```

## Documentation Files

### User Guides
- **HOW_TO_RUN_V9.md**: Complete guide for running and using V9 (CURRENT)
  - Multi-tab Excel workflow
  - Enhanced quality flags (stale + zero)
  - File archiving instructions
  - Branding and UI features
- **HOW_TO_USE.md**: General usage guide
- **README.md**: Project overview

### Legacy Documentation
- **HOW_TO_RUN_V6.md**: V6 guide (outdated, use V9)
- **AI_INTEGRATION.md**: V5 AI features (outdated)
- **DEBUG_WINDOW_GUIDE.md**: V5 debug window (outdated)
- **HOW_TO_RUN.md**: V5 instructions (outdated)

Note: V5-V9 documentation is kept for reference but V10 has significantly improved features and architecture.

## Key Differences Between Versions

### V11 → V12

**New Features:**
- **Multi-Column CSV Support**: CSV files can now have multiple value columns selected
  - AI prompt updated to request `value_columns` array instead of single `value_column`
  - Checkbox interface for column selection (matches Excel sheet workflow)
  - Column naming: `"{Filename} {ColumnName}"` prefix for uniqueness
  - Example: `sensor_data.csv` with "Temperature" column → `"sensor_data Temperature"`

**AI Response Format Changes:**
- **V11 CSV Format**:
  ```json
  {
    "delimiter": ",",
    "start_row": 1,
    "date_column": 0,
    "value_column": 2,
    "sensor_name": "Temperature"
  }
  ```
- **V12 CSV Format** (now matches multi-tab Excel):
  ```json
  {
    "delimiter": ",",
    "start_row": 1,
    "date_column": 0,
    "value_columns": [2, 3, 4],
    "column_names": ["Temperature", "Humidity", "Pressure"]
  }
  ```

**Config Structure Changes:**
- **V11 CSV Config**:
  ```python
  {
      'file_type': 'csv',
      'config': {
          'start_row': 1,
          'delimiter': ',',
          'date_column': 0,
          'value_column': 2,        # scalar
          'sensor_name': 'Temp'     # scalar
      }
  }
  ```
- **V12 CSV Config** (matches Excel pattern):
  ```python
  {
      'file_type': 'csv',
      'config': {
          'start_row': 1,
          'delimiter': ',',
          'date_column': 0,
          'available_columns': [2, 3, 4],              # array
          'column_names': ['Temp', 'Humidity', 'Fan'], # array
          'selected_columns': [2, 3]                   # user subset
      }
  }
  ```

**UI Changes:**
- **V11**: Single number input for "Value Column" + text input for "Sensor Name"
- **V12**: Checkbox grid (3 columns) for selecting multiple value columns
- **V12**: Preview shows all selected columns with filename prefix
- **V12**: Fallback interface for manual column addition if AI detection fails

**Data Extraction Changes:**
- **V11**: Extracts single column with sensor_name
- **V12**: Loops through `selected_columns` array, creates column per selection
- **V12**: Each column named `"{Filename} {ColumnName}"` (matches multi-tab Excel pattern)

**Backward Compatibility:**
- V12 auto-converts old single-column AI responses to new array format
- If AI returns `value_column` (scalar), converts to `value_columns: [value_column]`

### V10 → V11

**New Features:**
- **Fully Automatic Workflow**: Single button processes entire pipeline
  - Combines all files with progress tracking (0-40%)
  - Saves raw CSV to archive automatically (40%)
  - Resamples to quarter-hour with progress updates (40-80%)
  - Generates Excel with color coding automatically (80-100%)
  - No manual steps between combine/resample/export
- **Progress Tracking**: Real-time progress bar across all phases
  - Phase-weighted progress calculation
  - Progress callback in `resample_to_quarter_hour()` (updates every 100 rows)
  - Status messages show current operation
- **Automatic File Generation**: Files saved without user intervention
  - Raw CSV: `{Building}_raw_merged_{YYYYMMDD_HHMMSS}.csv`
  - Excel: `{Building}_resampled_15min_{YYYYMMDD_HHMMSS}.xlsx`
  - Both files saved to archive folder automatically
  - Filenames use sanitized building names
- **Streamlined 4-Step Process**: Reduced from 6 steps to 4
  - Removed: Manual "Combine" button (Step 4)
  - Removed: Manual "Resample" button (Step 5)
  - Removed: Manual "Export" button (Step 6)
  - Added: Single automatic "Process All Files" button
- **Removed AI Debug Panel**: 65 lines removed for cleaner interface
- **New Helper Functions**:
  - `auto_process_and_export()` - 200+ line orchestration function
  - `sanitize_building_name()` - Clean special characters from building names
  - `validate_archive_path()` - Validate and create archive directory

**Workflow Changes:**
- **V10**: Upload → AI → Review → Combine → [Download Raw] → Resample → Export (6 steps, 3 buttons)
- **V11**: Upload → AI → Review → Process & Export (4 steps, 1 button)

**File Output:**
- **V10**: Manual download buttons, user chooses filenames, saves to local Downloads
- **V11**: Automatic save to archive folder, timestamped filenames, download buttons for convenience

**UI After Processing:**
- **V10**: Shows preview, requires manual export steps
- **V11**: Shows summary metrics, download buttons, preview tabs, reset button

**Performance:**
- **V11**: Slightly slower due to automatic Excel generation, but more efficient workflow
- Progress tracking adds minimal overhead (<1% of total time)

**One-Shot Processing:**
- **V10**: Can re-run individual steps (combine, resample, export)
- **V11**: One-shot workflow, must reset to process again

**Config Structure:**
- Same as V10 (no changes needed)

### V9 → V10

**New Features:**
- **Tab-Based UI Redesign**: Replaces accordion interface with two-level tab system
  - File-level tabs with visual indicators (`✓ (count)`, `⚠️`, `❌`)
  - Sheet-level tabs for multi-tab Excel files (nested tabs)
  - No more scrolling through 40 open sections
  - Cleaner, more intuitive navigation
- **Smart Data Type Preservation**: Intelligent column type detection for SQL-ready output
  - 80% threshold algorithm: `smart_convert_column()`
  - Pure text columns ("off"/"on") preserved as text
  - Mostly numeric columns converted to numeric (dashes → NaN)
  - Mixed columns (<80% numeric) preserved as text
  - All reads use `dtype=str` + `keep_default_na=False`, then smart conversion
- **Improved Quality Flagging**: Only checks numeric columns
  - Stale data detection skips text fields (prevents false positives on fan status)
  - Zero value detection skips text fields
  - Uses `pd.api.types.is_numeric_dtype()` to identify column types
- **New Helper Functions**:
  - `build_tab_label()` - Dynamic tab labels with visual feedback
  - `smart_convert_column()` - Intelligent type conversion
  - `render_sheet_config_ui()` - Modular sheet configuration
  - `render_csv_config_ui()` - Modular CSV configuration

**UI Changes:**
- **V9**: Accordion interface with `st.expander(expanded=True)` - all sections open
- **V10**: Tab interface with `st.tabs()` - click to view each file/sheet

**Data Type Handling:**
- **V9**: `pd.to_numeric(errors='coerce')` on all columns - text lost
- **V10**: `dtype=str` read → `smart_convert_column()` - text preserved, SQL-ready

**Quality Flagging:**
- **V9**: Checks all columns for staleness/zeros (includes text fields)
- **V10**: Only checks numeric columns (skips text fields like "off"/"on")

**Performance:**
- **V10**: <1 second overhead for smart conversion
- Fast-path optimization for pure text columns

**Config Structure:**
- Same as V9 (no changes needed)

### V7 → V9

**New Features:**
- **Multi-Tab Excel Support**: Process Excel files with multiple worksheets
  - AI-powered tab detection and column identification
  - Dynamic column naming (`[TabName] [ColumnName]`)
  - Per-tab column selection UI with checkboxes
  - Preserves percentage formatting where possible
- **Enhanced Quality Flags**:
  - Modified stale flag: Now detects 3+ consecutive non-zero values (changed from 4+)
  - Stale flag only checks non-zero values (zeros tracked separately)
  - New `Zero_Value_Flag` column with values: "Clear", "Single", "Repeated"
  - `Stale_Sensors` column: Comma-separated list of problematic sensors
- **File Archiving System**:
  - Building Name input field for organization
  - Default archive: `archive/[Building Name]/`
  - Optional custom archive location via UI checkbox
  - Automatic file backup before processing
- **Fischer Energy Branding**:
  - Custom logo display (fischer background clear)
  - Teal color scheme (#24b3aa)
  - Custom CSS for buttons and headers
  - Theme configured via `.streamlit/config.toml`
- **Improved Column Ordering**: Date, Stale_Data_Flag, Stale_Sensors, Zero_Value_Flag, then sensors

**Workflow Changes:**
- **V7**: Upload → AI → Review → Combine → [Download Raw] → Resample → Export (6 steps)
- **V9**: Upload & Archive → AI → Review [+ Multi-Tab Selection] → Combine → [Download Raw] → Resample → Export (6 steps)

**Output Changes:**
- **V7**: Date, Inexact_Match_Flag, per-sensor stale flags, then sensors
- **V9**: Date, Stale_Data_Flag, Stale_Sensors, Zero_Value_Flag, then sensors (simplified, consolidated flags)

**Algorithm Changes:**
- Stale flag: 3+ consecutive (was 4+), non-zero only (was all values)
- Zero flag: New per-row tracking with priority logic
- Multi-tab data extraction with column selection
- Column reordering at end of resampling

**Config Structure:**
- **V7**: Flat config dictionary per file
- **V9**: Nested structure with file type detection
  - CSV/Single-Tab: `{'file_type': 'csv', 'config': {...}}`
  - Multi-Tab: `{'file_type': 'excel_multi_tab', 'tabs': {...}}`

### V6 → V7

**New Features:**
- **Quarter-Hour Resampling**: Automatic 15-minute interval resampling with `merge_asof()`
- **Quality Flags**: `Inexact_Match_Flag` and per-sensor `{Sensor}_Stale_Flag` columns
- **Two-Stage Export**: Raw merged CSV (optional) + resampled CSV
- **Resampling Statistics**: Detailed metrics on data quality and completeness
- **6-Step Workflow**: Added Step 5 (Resample) between Combine and Export

**Workflow Changes:**
- **V6**: Upload → AI → Review → Combine → Export (5 steps)
- **V7**: Upload → AI → Review → Combine → [Download Raw] → Resample → Export (6 steps)

**Output Changes:**
- **V6**: Single CSV with all original timestamps
- **V7**: Two CSVs - raw merged (optional) + resampled 15-min with flags

**Algorithm Addition:**
- Efficient `pd.merge_asof()` for nearest-value matching
- Vectorized flag operations for quality checks
- Automatic deduplication with `drop_duplicates()`

### V5 → V6

**Configuration Simplification:**
- **V5**: `skip_rows` + `header_row` (confusing, redundant)
- **V6**: Single `start_row` parameter (where headers are)

**Excel Time Column:**
- **V5**: Supported Excel Time column
- **V6**: Completely removed (was a data artifact)

**AI Processing:**
- **V5**: Sequential per-file analysis
- **V6**: Parallel batch analysis (5 concurrent)

**Timestamp Handling:**
- **V5**: Basic pandas parsing, inconsistent formats
- **V6**: Robust normalization to MM/DD/YYYY HH:MM:SS

**User Interface:**
- **V5**: Hidden previews in expanders, quick check boxes
- **V6**: Inline extracted preview with timestamp conversion example

**JSON Response:**
- **V5**: 7 fields including excel_time_column, skip_rows, header_row
- **V6**: 5 fields (simplified, cleaner)

## Best Practices

1. **Always use V12** for new work to get multi-column CSV support, automatic workflow, and streamlined 4-step process
2. **Set CLAUDE_API_KEY** in `.env` file before running
3. **Enter building name** for proper file archiving and automatic filename generation
4. **Use "Analyze All Files"** button to process all files in parallel
5. **Review extracted previews** to verify correct column detection
6. **Use tab interface** in Step 3 to navigate between files and sheets efficiently
7. **Select columns carefully** in both CSV and Excel files using checkboxes (V12 now supports multi-column CSV)
8. **Check timestamp conversion** examples before processing
9. **Click "Process All Files"** and watch the progress bar - the entire workflow is automatic
10. **Files are automatically saved** to the archive folder with timestamped filenames
11. **Use download buttons** after processing to get copies of the generated files
12. **Review quality statistics** in the preview tabs to understand data quality
13. **Use quality flags** to identify problematic data points:
    - Filter on `Stale_Data_Flag=False` for clean data (only checks numeric columns)
    - Check `Stale_Sensors` to identify stuck numeric sensors
    - Filter on `Zero_Value_Flag='Clear'` to exclude zero readings
    - Note: Text fields like "off"/"on" are automatically excluded from quality checks
14. **Verify data types** in output files - text columns preserved, numeric columns converted
15. **Keep sensor names unique** across files for clear column identification
16. **Check archive folder** for output files - they're saved with timestamps for easy tracking
17. **Use "Process Different Files"** button to reset and process a new batch

## Troubleshooting

### Import Errors
If you see `ModuleNotFoundError: No module named 'src'`:
- The app uses relative imports from the same directory
- Run from project root: `streamlit run src/app_v12.py`

### Timestamp Parsing Issues
If timestamps fail to normalize:
- Verify input format is supported (see timestamp_normalizer.py)
- Falls back to pandas parsing if custom normalization fails
- Check error messages in the UI after clicking "Process All Files"

### AI Detection Failures
If AI doesn't detect columns correctly:
- Verify file has at least 15 lines of data
- Manually adjust settings in Step 3 (all inputs are editable)
- Check API key is valid and has Sonnet 4.5 access

### Data Type Issues (V10+)
If text values like "off"/"on" appear as NaN or numbers in output:
- Verify you're using V12 (check page title shows "V12")
- Check if column meets 80% threshold for numeric detection
- Mixed columns with <80% numeric values are preserved as text
- If needed, adjust threshold in `smart_convert_column()` function

### Stale Data False Positives (V10+)
If text fields are flagged as stale:
- V12 automatically skips text columns in quality checks
- Only numeric columns are checked for staleness
- Verify column type is detected correctly (should show as object/string for text)

### Processing Errors (V11/V12)
If automatic processing fails:
- Check error message displayed in red
- Verify archive path exists and is writable
- Ensure sufficient disk space for output files
- Check that building name contains valid characters
- Review partial results in error message (may include which phase failed)

### Performance Issues
If processing is slow:
- V11/V12: Progress tracking adds <1% overhead
- Automatic Excel generation takes most time (normal)
- V6+ uses 5 parallel workers by default (adjustable in code)
- Large files (>100k rows) may take time to load
- Timestamp normalization is fast but processes every row
