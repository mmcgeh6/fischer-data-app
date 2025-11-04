# How to Run Fischer Data App V7

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set Up API Key
Create a `.env` file in the project root:
```bash
CLAUDE_API_KEY=sk-ant-your-api-key-here
```

### 3. Run the Application
```bash
streamlit run src/app_v7.py
```

The app will open in your default browser at `http://localhost:8501`

## Complete Workflow

### Step 1: Upload Files
- Click "Browse files" and select all your sensor CSV/Excel files
- Support for `.csv`, `.xlsx`, `.xls` files
- Can upload 10-90 files at once

### Step 2: AI Analysis
- Click "Analyze All Files" button
- Claude AI will analyze all files in parallel (5 concurrent workers)
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

### Step 5: Resample to Quarter-Hour Intervals ⭐ NEW in V7
- Click "Resample to 15-Minute Intervals"
- Creates complete 15-minute timestamp grid (:00, :15, :30, :45)
- Finds nearest data point within ±2 minute tolerance
- Adds quality flags:
  - **Inexact_Match_Flag**: Identifies timestamps not exactly on 15-min marks
  - **{Sensor}_Stale_Flag**: Detects 4+ consecutive identical readings per sensor
- Shows resampling statistics:
  - Total intervals created
  - Count/percentage of inexact matches
  - Stale data count per sensor

### Step 6: Export Resampled Data
- Enter output filename (default: `resampled_15min_YYYYMMDD_HHMMSS.csv`)
- Click "Generate Resampled CSV"
- Download the resampled data with quality flags
- View export statistics:
  - Data summary for all sensors
  - Quality flag counts
  - Missing data report

## What You Get

### Raw Merged CSV (Optional from Step 4)
- All sensors combined into one file
- Original timestamps preserved (minute-level)
- Columns: `Date`, `Sensor1`, `Sensor2`, ..., `SensorN`

### Resampled CSV (from Step 6)
- 15-minute interval timestamps only
- All quality flags included
- Columns:
  - `Date` (15-min intervals: :00, :15, :30, :45)
  - `Sensor1`, `Sensor2`, ..., `SensorN` (sensor values)
  - `Inexact_Match_Flag` (Boolean)
  - `Sensor1_Stale_Flag`, `Sensor2_Stale_Flag`, ... (Boolean per sensor)

## Understanding Quality Flags

### Inexact_Match_Flag
**When True:**
- Original data timestamp was not exactly on a 15-minute mark
- Data point had non-zero seconds (e.g., 12:30:05)
- No data found within ±2 minute tolerance window

**Use this to:**
- Identify interpolated/estimated values
- Filter for exact matches only: `df[df['Inexact_Match_Flag'] == False]`

### {Sensor}_Stale_Flag
**When True:**
- Sensor value is identical to previous 3 values (4 consecutive identical readings)
- May indicate stuck or malfunctioning sensor

**Use this to:**
- Identify potentially problematic sensor data
- Flag sensors for maintenance/inspection
- Filter out stale readings in analysis

## AI Debug Panel

Located at the bottom of the app, shows:
- Total API calls and success rate
- Individual file analysis details:
  - Request parameters (model, tokens, prompt)
  - Response (parsed JSON configuration)
  - Error messages (if any)
- Use this to troubleshoot AI detection issues

## Tips for Best Results

1. **File Naming**: Use descriptive sensor names in filenames (AI extracts sensor names)
2. **Consistent Format**: Files with similar structure process more reliably
3. **Review AI Settings**: Always check Step 3 before combining
4. **Download Raw First**: If unsure about resampling, download raw merged CSV first
5. **Check Statistics**: Review resampling stats before final export
6. **Use Quality Flags**: Filter data based on your quality requirements

## Troubleshooting

### AI Detection Issues
- Check debug panel for API response
- Manually adjust settings in Step 3
- Ensure file has recognizable header row with "Date" and "Value" columns

### Timestamp Parsing Errors
- V7 supports multiple formats automatically
- Check timestamp conversion preview in Step 3
- If issues persist, check raw file for unusual date formats

### Performance with Large Files
- V7 is optimized for 10-90 files × 100-600,000 rows each
- AI analysis: ~1-3 seconds per file (5 parallel)
- Resampling: O(n log n) using merge_asof (efficient)
- Memory: ~2-4 GB for 54M rows target

### Missing API Key
If you see "CLAUDE_API_KEY not found":
1. Create `.env` file in project root
2. Add: `CLAUDE_API_KEY=sk-ant-your-key-here`
3. Restart the app

## Example Output

**Resampled Data Structure:**
```csv
Date,Sensor_A,Sensor_B,Inexact_Match_Flag,Sensor_A_Stale_Flag,Sensor_B_Stale_Flag
07/18/2024 12:00:00,72.5,45.2,False,False,False
07/18/2024 12:15:00,72.5,45.3,False,False,False
07/18/2024 12:30:00,72.5,45.3,False,False,False
07/18/2024 12:45:00,72.5,45.3,True,True,False
07/18/2024 13:00:00,73.1,45.4,False,False,False
```

In this example:
- Row 4 (12:45:00) has Inexact_Match_Flag=True (original data not exactly at 12:45:00)
- Row 4 has Sensor_A_Stale_Flag=True (4 consecutive identical values: 72.5)

## Next Steps

After exporting resampled data:
1. **Data Analysis**: Use quality flags to filter/clean data
2. **Visualization**: Plot sensor trends at 15-minute intervals
3. **Reporting**: Identify sensors with high stale flag counts for maintenance
4. **Database Integration**: Import to SQL for further analysis (future feature)

## Need Help?

- Review [CLAUDE.md](CLAUDE.md) for architecture details
- Check [README.md](README.md) for project overview
- Run test: `python test_v7_resampling.py`
