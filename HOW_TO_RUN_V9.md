# How to Run Fischer Data App V9

## Overview

Fischer Data App V9 is a Streamlit-based application for processing building management system (BMS) sensor data. It combines multiple Excel/CSV files, performs quarter-hour resampling, and provides comprehensive data quality flagging.

**New in V9:**
- ✅ Enhanced stale data detection (3+ consecutive non-zero values)
- ✅ New Zero Value Flag for tracking zero readings
- ✅ Multi-tab Excel file support with column selection
- ✅ Automatic file archiving with building name organization
- ✅ Fischer Energy branding and custom styling
- ✅ Improved column ordering (Date, flags, then sensors)

---

## Prerequisites

### 1. Python Installation
- Python 3.8 or higher
- pip package manager

### 2. Required Packages
Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

**Key Dependencies:**
- `streamlit>=1.28.0` - Web UI framework
- `pandas>=2.0.0` - Data processing
- `openpyxl>=3.1.0` - Excel file support
- `anthropic>=0.18.0` - Claude AI API
- `python-dotenv>=1.0.0` - Environment variables

### 3. Claude API Key Setup

Create a `.env` file in the project root:

```bash
CLAUDE_API_KEY=sk-ant-api03-...
```

Get your API key from: https://console.anthropic.com/

---

## Running the Application

### Start the App

```bash
streamlit run src/app_v9.py
```

The app will open in your default browser at `http://localhost:8501`

### Stop the App

Press `Ctrl+C` in the terminal running the app.

---

## User Workflow (6 Steps)

### **Step 1: Upload Files & Archive Settings**

1. **Enter Building Name** (required)
   - Used for organizing archived files
   - Example: "Gotham Tower", "City Hall", "Hospital East"

2. **Choose Archive Location**
   - **Default**: Files saved to `archive/[Building Name]/`
   - **Custom**: Check "Use custom archive location" to specify a different path
     - Examples: `C:/MyFiles/Archive`, `D:/Backups/BuildingData`

3. **Upload Files**
   - Click "📁 Upload your sensor files"
   - Select multiple files (CSV, XLS, or XLSX)
   - Supports 10-90 files per session
   - Files are automatically archived to the specified location

**Archive Behavior:**
- All uploaded files are **always archived** before processing
- Original files remain untouched in the archive
- Processed data is kept separate from originals

---

### **Step 2: AI Analysis**

1. **Click "Analyze All Files"**
   - Claude AI analyzes each file in parallel (5 concurrent workers)
   - Detects file type (CSV vs Excel single-tab vs Excel multi-tab)
   - Identifies column structure, delimiters, and data format
   - Typically completes in 1-3 seconds per file

2. **Review AI Detection Results**
   - ✅ Green checkmarks indicate successful analysis
   - 📊 Shows detected configuration for each file
   - 🔍 Expand "AI Debug Log" to see detailed API requests/responses

**What AI Detects:**

For **CSV and Single-Tab Excel Files:**
- `start_row`: Header row location (0-based index)
- `delimiter`: Column separator (comma, tab, semicolon, pipe)
- `date_column`: Date/timestamp column index
- `value_column`: Sensor value column index
- `sensor_name`: Extracted from metadata or filename

For **Multi-Tab Excel Files:**
- Per-tab configuration (start_row, date_column)
- Multiple value columns per tab
- Column names for dynamic naming

---

### **Step 3: Review & Edit Configuration**

Review the detected settings for each file. All values are editable if AI detection is incorrect.

#### **CSV / Single-Tab Excel Files:**

Edit any of these fields:
- **Start Row**: Row where column headers are located (0-based)
- **Delimiter**: Column separator character
- **Date Column**: Index of timestamp column (0-based)
- **Value Column**: Index of sensor reading column
- **Sensor Name**: Display name for this sensor in output

**Preview Section:**
- Shows first 5 rows of extracted data
- Displays timestamp conversion example
- Verify data is being read correctly

#### **Multi-Tab Excel Files:**

For files with multiple tabs, configure each tab separately:

1. **Expand Each Tab Section**
   - Shows tab name and configuration

2. **Select Date Column**
   - Choose which column contains timestamps

3. **Select Value Columns**
   - ✅ Check boxes for columns to include
   - Can select multiple columns per tab
   - Uncheck columns you don't need

4. **Column Naming**
   - Final names: `[Tab Name] [Column Name]`
   - Example: "AC12-1 Return Air Temp", "AC12-1 Supply Air Temp"

**Multi-Tab Preview:**
- Shows detected tabs and column options
- Displays sample data for verification
- All settings are editable

---

### **Step 4: Combine Files**

1. **Click "Combine All Files"**
   - Loads data from all configured files
   - Normalizes timestamps to `MM/DD/YYYY HH:MM:SS` format
   - Performs outer merge on Date column
   - Removes duplicate rows

2. **Review Combined Data**
   - Shows first 50 rows of merged data
   - All sensors aligned by timestamp
   - Missing data shown as blank cells (not "null")

**Data Combining Details:**

- **CSV/Single-Tab Files**: Extract Date + Value columns
- **Multi-Tab Files**: Extract Date + multiple Value columns per tab
- **Timestamp Normalization**: Handles various input formats:
  - US-style: `7/18/2024 12:45:00 PM EDT`
  - 24-hour: `07/18/2024 14:30:00`
  - Text month: `July 18, 2024 2:30 PM`
  - ISO-style: `2024-07-18 14:30:00`
- **Output Format**: Always `MM/DD/YYYY HH:MM:SS` (24-hour)

**Optional: Download Raw Merged CSV**
- Click "Download Raw Merged Data (Before Resampling)"
- Preserves all original timestamps (no resampling yet)
- Useful for minute-level data preservation

---

### **Step 5: Resample to Quarter-Hour**

1. **Click "Resample to 15-Minute Intervals"**
   - Creates complete 15-minute timestamp grid (:00, :15, :30, :45)
   - Uses `merge_asof` algorithm for nearest-value matching
   - Matches within ±2 minute tolerance
   - Applies quality flagging

2. **Review Resampling Statistics**
   - Total 15-minute intervals created
   - Inexact match count and percentage
   - Stale data count per sensor
   - Date range coverage

**Quality Flags Added:**

#### **Stale_Data_Flag** (Boolean - per row)
- `True`: At least one sensor has stale data on this row
- `False`: All sensors have fresh/valid data
- Uses per-sensor stale detection

#### **Stale_Sensors** (Text - per row)
- Comma-separated list of sensors with stale readings
- Example: "Sensor A, Sensor C"
- Empty if no stale data detected

**Stale Data Logic (V9):**
- Flags when current value equals previous **2 values** (3 consecutive identical)
- Only checks **non-zero values** (zeros are handled separately)
- Each sensor tracked independently
- Example: `[10, 10, 10]` → row 3 flagged as stale

#### **Zero_Value_Flag** (Text - per row)
- `"Clear"`: No zero values detected in any sensor
- `"Single"`: One or more sensors have a zero, but not consecutive
- `"Repeated"`: One or more sensors have 2+ consecutive zeros

**Zero Flag Logic:**
- Checks all sensors for zero values
- Prioritizes "Repeated" over "Single"
- Flags consecutive zeros per sensor
- Example timeline for a sensor:
  - Row 1: `5` → Clear
  - Row 2: `0` → Single (first zero)
  - Row 3: `0` → Repeated (second consecutive zero)
  - Row 4: `3` → Clear (non-zero)
  - Row 5: `0` → Single (isolated zero)

---

### **Step 6: Export Data**

1. **Click "Download Resampled Data with Flags"**
   - Downloads CSV with all quality flags
   - Standard 15-minute intervals
   - Ready for analysis

**Output File Structure:**

```csv
Date,Stale_Data_Flag,Stale_Sensors,Zero_Value_Flag,Sensor A,Sensor B,Sensor C
01/15/2024 00:00:00,False,,Clear,72.5,55.3,45.8
01/15/2024 00:15:00,False,,Single,72.5,0,45.8
01/15/2024 00:30:00,True,Sensor A,Repeated,72.5,0,45.8
01/15/2024 00:45:00,True,Sensor A,Clear,72.5,55.3,45.8
```

**Column Order:**
1. `Date` - Timestamp in MM/DD/YYYY HH:MM:SS format
2. `Stale_Data_Flag` - Boolean (TRUE/FALSE)
3. `Stale_Sensors` - Comma-separated sensor names or blank
4. `Zero_Value_Flag` - Text (Clear/Single/Repeated)
5. All sensor columns (alphabetically sorted)

---

## Multi-Tab Excel Workflow

### When to Use Multi-Tab Processing

Multi-tab Excel files are detected automatically when:
- File has `.xlsx` or `.xls` extension
- File contains 2+ worksheet tabs
- Common in projects with multiple sensor groups

### Example Multi-Tab Structure

**File: `HVAC_System_AC12.xlsx`**

**Tab 1: "AC12-1"**
```
Date                    | Return Air Temp | Supply Air Temp | Fan Status
01/15/2024 00:00:00    | 72.5           | 55.3           | 1
01/15/2024 00:01:00    | 72.6           | 55.4           | 1
```

**Tab 2: "AC12-2"**
```
Date                    | Return Air Temp | Supply Air Temp | Fan Status
01/15/2024 00:00:00    | 68.2           | 52.1           | 1
01/15/2024 00:01:00    | 68.3           | 52.2           | 1
```

### Column Selection Process

1. **AI Analysis Detects**:
   - Tab names: "AC12-1", "AC12-2"
   - Date column: index 0
   - Available value columns: [1, 2, 3]
   - Column names: ["Return Air Temp", "Supply Air Temp", "Fan Status"]

2. **User Selects Columns** (Step 3):
   - Check boxes for columns to include
   - Example: Select columns 1 and 2, skip column 3

3. **Final Column Names**:
   - `AC12-1 Return Air Temp`
   - `AC12-1 Supply Air Temp`
   - `AC12-2 Return Air Temp`
   - `AC12-2 Supply Air Temp`

### Percentage Format Preservation

For columns detected as percentages:
- AI attempts to detect percentage formatting
- Values preserved as-is (not converted to decimals)
- Example: `85%` stays as `85` in output

---

## Archive Organization

### Default Structure

```
archive/
└── Gotham Tower/
    ├── sensor_file_1.csv
    ├── sensor_file_2.xlsx
    └── sensor_file_3.csv
```

- Clean, permanent folder structure
- Building name determines subfolder
- No date stamps in folder names
- Easy to navigate and manage

### Custom Archive Locations

Check **"Use custom archive location"** to save files elsewhere:

**Examples:**
- Network drive: `//server/shared/BuildingData`
- External drive: `D:/Backups/Sensors`
- Cloud sync folder: `C:/Users/Name/OneDrive/BMS Archive`

**Best Practices:**
- Use absolute paths (e.g., `C:/Folder` not `Folder`)
- Ensure folder is writable
- Avoid spaces in paths (use quotes if necessary)

---

## Quality Flag Usage Examples

### Finding Problematic Data

**Filter for clean data only:**
```python
import pandas as pd

df = pd.read_csv('output.csv')

# Only exact 15-minute intervals with no stale data
clean_data = df[df['Stale_Data_Flag'] == False]

# No zero values detected
no_zeros = df[df['Zero_Value_Flag'] == 'Clear']

# Perfect data: no stale, no zeros
perfect = df[(df['Stale_Data_Flag'] == False) & (df['Zero_Value_Flag'] == 'Clear')]
```

**Identify stuck sensors:**
```python
# Find which sensors are problematic
stale_sensors = df[df['Stale_Data_Flag'] == True]['Stale_Sensors'].value_counts()
print(stale_sensors)

# Output example:
# Sensor A, Sensor C    15
# Sensor B              8
# Sensor A              3
```

**Track zero value patterns:**
```python
# Count zero flag occurrences
zero_counts = df['Zero_Value_Flag'].value_counts()
print(zero_counts)

# Output example:
# Clear       1200
# Single      45
# Repeated    12
```

---

## Troubleshooting

### AI Analysis Issues

**Problem**: AI detection fails or returns incorrect configuration

**Solutions:**
1. Check `.env` file has valid `CLAUDE_API_KEY`
2. Expand "AI Debug Log" to see error details
3. Manually edit configuration in Step 3
4. Verify file has at least 15 lines of data
5. Check for unusual file encoding (use UTF-8 if possible)

---

### File Upload Issues

**Problem**: Files not uploading or archiving fails

**Solutions:**
1. Verify building name is entered (required for archiving)
2. Check archive path is valid and writable
3. Ensure no special characters in building name
4. Try custom archive location if default fails
5. Check disk space availability

---

### Timestamp Parsing Issues

**Problem**: Timestamps not normalizing correctly

**Solutions:**
1. Check preview section in Step 3 for conversion examples
2. Verify date column selection is correct
3. Review AI Debug Log for parsing errors
4. Supported formats are listed in Step 4 documentation above
5. Falls back to pandas parsing if custom normalization fails

---

### Multi-Tab Column Selection Issues

**Problem**: Columns not appearing or incorrectly named

**Solutions:**
1. Verify tab configuration shows correct column count
2. Check start_row is set to header row (not data row)
3. Ensure date column is selected correctly
4. Review extracted preview to verify column detection
5. Manually adjust column indices if AI detection is off

---

### Resampling Issues

**Problem**: Too many inexact matches or unexpected flags

**Solutions:**
1. Review resampling statistics in Step 5
2. Check input data timestamp precision (should be minute-level or better)
3. Verify tolerance setting (default ±2 minutes)
4. Use raw merged CSV to inspect original timestamps
5. Consider if data source already provides 15-minute intervals

---

### Performance Issues

**Problem**: App is slow or unresponsive

**Solutions:**
1. V9 processes files in parallel (5 concurrent workers)
2. Large files (>100k rows) may take time to load
3. Close other browser tabs to free memory
4. Restart the Streamlit app (`Ctrl+C` then re-run)
5. Check system RAM availability (large datasets use 2-4 GB)

---

## Advanced Features

### Session State Management

The app maintains state between interactions:
- Uploaded files persist until browser refresh
- Configuration changes are saved automatically
- Combined data is cached for efficiency
- Resampling results preserved for download

**To Reset App:**
- Refresh browser page (clears session state)
- Or restart Streamlit server

---

### Debug Mode

**AI Debug Log** (bottom of Step 2):
- Shows all API requests and responses
- Includes prompt text and token counts
- Displays parsed JSON for each file
- Useful for troubleshooting AI detection issues

---

### Custom Branding

V9 uses Fischer Energy brand colors:
- **Teal Primary**: `#24b3aa`
- **White Background**: `#FFFFFF`
- **Black Text**: `#151515`

Configured in `.streamlit/config.toml` and custom CSS injection.

---

## File Locations

### Input Files
- Uploaded files temporarily stored in: `temp/`
- Original files archived to: `archive/[Building Name]/` (default)

### Output Files
- Raw merged CSV: `combined_data_[timestamp].csv`
- Resampled CSV: `resampled_data_[timestamp].csv`

### Configuration
- API key: `.env`
- Theme colors: `.streamlit/config.toml`
- Logo: `assets/fischer background clear (1).png`

---

## Best Practices

1. **Always enter a building name** - Required for proper archiving
2. **Use AI analysis** - Faster and more accurate than manual config
3. **Review extracted previews** - Verify data before combining
4. **Check resampling statistics** - Understand data quality metrics
5. **Use quality flags** - Filter data based on your analysis needs
6. **Download raw merged CSV** - Keep original minute-level data if needed
7. **Organize archives** - Use clear building names for easy navigation
8. **Monitor debug log** - Check for API errors or detection issues

---

## Version History

### V9 (Current)
- Enhanced stale flag logic (3+ consecutive non-zero)
- New Zero_Value_Flag column (Clear/Single/Repeated)
- Multi-tab Excel support with column selection
- Automatic file archiving with building name organization
- Simplified archive structure: `archive/[Building Name]/`
- Custom archive location picker
- Fischer Energy branding
- Improved column ordering

### V8
- Quarter-hour resampling with merge_asof
- Stale data flagging (4+ consecutive)
- Inexact match detection
- Parallel AI processing
- Timestamp normalization

### Earlier Versions
- See `CLAUDE.md` for complete version history

---

## Getting Help

### Common Commands
```bash
# Run the app
streamlit run src/app_v9.py

# Install dependencies
pip install -r requirements.txt

# Check Python version
python --version

# Update packages
pip install --upgrade -r requirements.txt
```

### Documentation Files
- `HOW_TO_RUN_V9.md` (this file) - Complete user guide
- `CLAUDE.md` - Technical documentation for developers
- `README.md` - Project overview

### Support
For issues or questions:
1. Check this documentation first
2. Review AI Debug Log for errors
3. Try manual configuration in Step 3
4. Verify all prerequisites are met
5. Consult `CLAUDE.md` for technical details

---

**Fischer Data App V9** - Built with Streamlit, Pandas, and Claude AI
