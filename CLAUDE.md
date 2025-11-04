# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fischer Data App V1 is a Streamlit-based data processing application for combining building management system sensor data from multiple Excel/CSV files. The app performs time-series alignment, timestamp normalization, quarter-hour resampling, and data quality flagging.

**LATEST (V7)**: Quarter-hour resampling with quality flags, building on V6's parallel AI analysis and timestamp normalization.

## Common Commands

### Running the Application
```bash
# Main application (latest version - V7 with quarter-hour resampling)
streamlit run src/app_v7.py

# Alternative versions
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

- **`app_v7.py`** (CURRENT - RECOMMENDED):
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

**V7 (Current):**
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
- `file_configs`: Dictionary of file configurations (start_row, delimiter, date_column, value_column, sensor_name)
- `combined_df`: Raw merged DataFrame (all sensors combined)
- `resampled_df`: 15-minute resampled DataFrame with quality flags (V7 only)
- `resampling_stats`: Dictionary with resampling metrics (V7 only)
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
- Date column (standardized to MM/DD/YYYY HH:MM:SS)
- Multiple sensor value columns (one per input file)
- All timestamps aligned on Date

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

## Performance Considerations

- Current: Handles ~12 files × 100 rows each efficiently
- Target: Designed for 10-90 files × 100-600,000 rows each
- Memory: ~54M rows fits in 2-4 GB RAM
- All pandas operations use optimized C implementations
- **V6 AI Analysis**: ~1-3 seconds per file, runs 5 files in parallel (ThreadPoolExecutor)
- **Timestamp Normalization**: Adds minimal overhead (~0.1ms per timestamp)
- **V7 Resampling**: `merge_asof()` is O(n log n), efficient for large datasets
- **V7 Flag Operations**: Vectorized pandas operations, very fast

## Dependencies

### Required Packages (requirements.txt)
```
pandas>=2.0.0           # Data processing
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
- **HOW_TO_RUN_V6.md**: Complete guide for running and using V6
- **HOW_TO_USE.md**: General usage guide
- **README.md**: Project overview

### Legacy Documentation
- **AI_INTEGRATION.md**: V5 AI features (outdated)
- **DEBUG_WINDOW_GUIDE.md**: V5 debug window (outdated)
- **HOW_TO_RUN.md**: V5 instructions (outdated)

Note: V5-specific documentation is kept for reference but V6 has significantly improved architecture.

## Key Differences Between Versions

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

1. **Always use V7** for new work to get quarter-hour resampling and quality flags
2. **Set CLAUDE_API_KEY** in `.env` file before running
3. **Use "Analyze All Files"** button to process all files in parallel
4. **Review extracted previews** to verify correct column detection
5. **Check timestamp conversion** examples before combining
6. **Download raw merged CSV** if you need minute-level data preservation
7. **Review resampling statistics** before exporting to understand data quality
8. **Use quality flags** to identify problematic data points:
   - Filter on `Inexact_Match_Flag=False` for exact 15-min timestamps only
   - Check `{Sensor}_Stale_Flag` columns to find stuck sensors
9. **Use debug panel** to troubleshoot AI detection issues
10. **Keep sensor names unique** across files for clear column identification

## Troubleshooting

### Import Errors
If you see `ModuleNotFoundError: No module named 'src'`:
- The app uses relative imports from the same directory
- Run from project root: `streamlit run src/app_v6.py`

### Timestamp Parsing Issues
If timestamps fail to normalize:
- Check the debug panel for error messages
- Verify input format is supported (see timestamp_normalizer.py)
- Falls back to pandas parsing if custom normalization fails

### AI Detection Failures
If AI doesn't detect columns correctly:
- Check debug panel for raw AI response
- Verify file has at least 15 lines of data
- Manually adjust settings in Step 3 (all inputs are editable)
- Check API key is valid and has Sonnet 4.5 access

### Performance Issues
If processing is slow:
- V6 uses 5 parallel workers by default (adjustable in code)
- Large files (>100k rows) may take time to load
- Timestamp normalization is fast but processes every row
