# How to Run Fischer Data App V8

## What's New in V8

**Excel Output with Visual Quality Indicators:**
- **Color-coded cells** instead of separate flag columns
- **Yellow highlighting**: Cells with inexact matches (not exactly on quarter-hour mark)
- **Light red highlighting**: Rows with stale data
- **Consolidated stale flags**: Single column with comma-separated sensor list
- Cleaner, more readable output format

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

Required packages include:
- pandas, openpyxl (data processing)
- streamlit (web interface)
- anthropic (AI analysis)
- python-dotenv (API key management)

### 2. Set Up API Key
Create a `.env` file in the project root:
```bash
CLAUDE_API_KEY=sk-ant-your-api-key-here
```

### 3. Run the Application
```bash
streamlit run src/app_v8.py
```

The app will open in your default browser at `http://localhost:8501`

## Complete Workflow

### Step 1: Upload Files
- Click "Browse files" and select all your sensor CSV/Excel files
- Support for `.csv`, `.xlsx`, `.xls` files
- Can upload 10-90 files at once

### Step 2: AI Analysis
- Click "Analyze All Files" button
- Claude AI analyzes all files in parallel (5 concurrent workers)
- Automatically detects:
  - File delimiter (comma, tab, etc.)
  - Header row location
  - Date/timestamp column
  - Sensor value column
  - Sensor name from metadata

### Step 3: Review & Edit Configurations
- Review AI-detected settings for each file
- Edit any incorrect settings:
  - Start Row (where headers are located)
  - Delimiter
  - Date Column (0-based index)
  - Value Column (0-based index)
  - Sensor Name
- Preview shows extracted data with timestamp conversion example
- All timestamps will be normalized to `MM/DD/YYYY HH:MM:SS` format

### Step 4: Combine All Data (Raw Merge)
- Click "Combine All Files"
- Performs outer join on Date column
- All timestamps normalized to standard format
- Removes exact duplicate rows
- **Download Raw Merged CSV (Optional)**:
  - Preserves all original timestamps (minute-level data)
  - Use this if you need the complete dataset before resampling

### Step 5: Resample to Quarter-Hour Intervals
- Click "Resample to 15-Minute Intervals"
- **Per-sensor independent matching**: Each sensor finds its own nearest value within ±2 minutes
- Creates complete 15-minute timestamp grid (:00, :15, :30, :45)
- Tracks which cells are inexact for color-coding
- Consolidates stale flags into two columns:
  - `Stale_Data_Flag`: True/False for each row
  - `Stale_Sensors`: Comma-separated list of sensors with stale data

### Step 6: Export to Excel ⭐ NEW in V8
- Enter output filename (default: `resampled_15min_YYYYMMDD_HHMMSS.xlsx`)
- Click "Generate Excel File"
- Download the Excel file with:
  - **Yellow cells**: Inexact matches
  - **Light red cells**: Rows with stale data
  - Clean data structure without extra flag columns

## Output Format

### Excel File Structure

**Columns:**
- `Date` (MM/DD/YYYY HH:MM:SS format)
- `Sensor1`, `Sensor2`, ..., `SensorN` (sensor values)
- `Stale_Data_Flag` (Boolean - True if ANY sensor has stale data)
- `Stale_Sensors` (Comma-separated list like "Sensor_A, Sensor_C")

**Visual Indicators:**
- **Yellow background**: Cell value is from a timestamp not exactly on quarter-hour mark
- **Light red background**: Row has one or more sensors with stale data (4+ consecutive identical values)

### Example Output

```
Date                 | Sensor_A | Sensor_B | Stale_Data_Flag | Stale_Sensors
---------------------|----------|----------|-----------------|---------------
07/18/2024 12:00:00 | 72.5     | 45.2     | False           |
07/18/2024 12:15:00 | 72.5*    | 45.3     | False           |
07/18/2024 12:30:00 | 72.5     | 45.3     | False           |
07/18/2024 12:45:00 | 72.5     | 45.3*    | True**          | Sensor_A
07/18/2024 13:00:00 | 73.1     | 45.4     | False           |
```

Legend:
- `*` = Yellow cell (inexact match - pulled from non-quarter-hour timestamp)
- `**` = Light red cell (stale data detected)

## Understanding Quality Indicators

### Yellow Cells (Inexact Matches)
**What it means:**
- The value was pulled from a timestamp that was NOT exactly on the quarter-hour mark
- Example: For 12:15:00 target, value came from 12:16:30
- Still within ±2 minute tolerance

**When to use:**
- Identify interpolated/estimated values
- Assess data quality and coverage
- Determine if sensor logging frequency needs adjustment

### Light Red Rows (Stale Data)
**What it means:**
- One or more sensors have 4 consecutive identical readings
- May indicate stuck or malfunctioning sensor
- `Stale_Sensors` column shows which specific sensors

**When to use:**
- Flag sensors for maintenance/inspection
- Filter out potentially bad data in analysis
- Monitor sensor health over time

### Consolidated Stale Flags vs V7
**V7 (old):**
- Separate `Sensor_A_Stale_Flag`, `Sensor_B_Stale_Flag`, etc.
- Many extra columns

**V8 (new):**
- Single `Stale_Data_Flag` column (True/False)
- Single `Stale_Sensors` column ("Sensor_A, Sensor_C")
- Cleaner, easier to read

## Comparison: V7 vs V8

| Feature | V7 | V8 |
|---------|----|----|
| Output format | CSV | Excel (.xlsx) |
| Inexact matches | Separate flag columns | Color-coded yellow cells |
| Stale data flags | Per-sensor columns | Consolidated: 1 boolean + 1 list |
| Visual indicators | None | Yellow (inexact) + Red (stale) |
| Readability | Many extra columns | Clean, minimal columns |
| Per-sensor matching | ✅ Yes | ✅ Yes |
| Timestamp normalization | ✅ Yes | ✅ Yes |
| AI analysis | ✅ Yes | ✅ Yes |

## Tips for Best Results

1. **Review Color-Coded Cells**: Check yellow cells to understand data quality
2. **Monitor Stale Sensors**: Use `Stale_Sensors` column to identify problematic sensors
3. **Download Raw First**: If unsure about resampling, download raw merged CSV first
4. **Excel Filtering**: Use Excel's built-in filtering on `Stale_Data_Flag` column
5. **Conditional Formatting**: Excel preserves colors when copying/filtering

## Troubleshooting

### Excel File Won't Open
- Ensure openpyxl is installed: `pip install openpyxl`
- Check file isn't locked by another program
- Verify output directory has write permissions

### Colors Not Showing
- Open file in Excel (not text editor)
- Check Excel version supports .xlsx format
- Try re-generating the file

### Performance with Large Datasets
- V8 uses same efficient algorithm as V7
- AI analysis: ~1-3 seconds per file (5 parallel)
- Resampling: O(n log n) per sensor
- Excel writing: ~1-2 seconds for typical datasets

### Missing API Key
If you see "CLAUDE_API_KEY not found":
1. Create `.env` file in project root
2. Add: `CLAUDE_API_KEY=sk-ant-your-key-here`
3. Restart the app

## Advanced Usage

### Customizing Colors
Edit `src/app_v8.py` in the `export_to_excel()` function:
```python
# Line 416-417
yellow_fill = PatternFill(start_color="FFFF00", ...)  # Change color code
red_fill = PatternFill(start_color="FFB6C1", ...)     # Change color code
```

### Adjusting Tolerance Window
Default: ±2 minutes. To change:
```python
# In Step 5 resampling call (line 904)
resampled, stats, inexact_cells = resample_to_quarter_hour(
    st.session_state.combined_df,
    tolerance_minutes=3  # Change from 2 to 3
)
```

### Changing Stale Threshold
Default: 4 consecutive identical values. Modify in `resample_to_quarter_hour()`:
```python
# Line 347-352
is_stale = (
    (resampled[sensor] == resampled[sensor].shift(1)) &
    (resampled[sensor] == resampled[sensor].shift(2)) &
    (resampled[sensor] == resampled[sensor].shift(3))  # Add .shift(4) for 5 consecutive
)
```

## Next Steps

After exporting Excel data:
1. **Visual Review**: Scan for yellow/red cells to spot data quality issues
2. **Filter by Stale_Data_Flag**: Isolate problematic rows
3. **Sensor Maintenance**: Use `Stale_Sensors` column to create maintenance list
4. **Data Analysis**: Import cleaned data into analysis tools
5. **Reporting**: Use color-coded Excel for stakeholder reports

## Need Help?

- Review [CLAUDE.md](CLAUDE.md) for architecture details
- Check [README.md](README.md) for project overview
- Compare with [HOW_TO_RUN_V7.md](HOW_TO_RUN_V7.md) for V7 differences
