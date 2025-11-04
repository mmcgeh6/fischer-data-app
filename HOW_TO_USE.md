# How to Use the Fischer Data Processing App

## Quick Start (For Beginners)

### Step 1: Open Terminal
1. Open Command Prompt or PowerShell
2. Navigate to this folder:
   ```
   cd "C:\Users\minke\OneDrive\Desktop\Fischer Data App V1"
   ```

### Step 2: Launch the App
Type this command and press Enter:
```
streamlit run src/app.py
```

A web browser will automatically open with the application!

---

## Using the Application

### Tab 1: Upload Files
1. Click **"Browse files"** button
2. Select your sensor data files (.xlsx or .csv)
   - You can select multiple files at once (Ctrl+Click)
3. Click **"Scan Files"** to validate them
4. Review the validation results
5. If everything looks good, proceed to Tab 2

### Tab 2: Process Data
1. Review your settings:
   - **Time Tolerance**: How far (±minutes) from 15-minute mark to search for data
   - **Stale Data Threshold**: How many consecutive repeats flag as "stale"
2. Click **"Start Processing"**
3. Watch the progress bar
4. When complete, go to Tab 3

### Tab 3: Results & Export
1. View the processing summary (total rows, sensors, flags, etc.)
2. Preview the clean data
3. Download your files:
   - **Clean 15-Minute Data**: The final output CSV (this is what you want!)
   - **Minute-Level Data**: Raw combined data (for future SQL database)

---

## What the App Does

### Input
- Takes 10-90 sensor export files from building management systems
- Each file has timestamps and sensor values

### Processing
1. **Loads** each file and extracts sensor name
2. **Combines** all sensors into one dataset matched by timestamp
3. **Saves minute-level data** (currently to CSV, future: SQL database)
4. **Resamples** to 15-minute intervals (00, 15, 30, 45 minutes)
5. **Flags** data quality issues:
   - Inexact time matches (not exactly on 15-min mark)
   - Stale data (sensor values repeated >3 times)

### Output
- Clean CSV file with:
  - All sensors in one file
  - 15-minute interval timestamps
  - Quality flags
  - Ready for analysis!

---

## Troubleshooting

### "Module not found" error
Run: `pip install -r requirements.txt`

### Port already in use
Close any other Streamlit apps, or use:
`streamlit run src/app.py --server.port 8502`

### Files not uploading
- Make sure files are .xlsx or .csv format
- Check that files have Date and Value columns

---

## File Structure

```
Fischer Data App V1/
├── CSVdata/           # Your sample input files
├── src/
│   ├── app.py         # Main GUI application
│   └── data_processor.py  # Core processing logic
├── output/            # Generated clean CSV files
├── temp/              # Temporary file storage (auto-created)
└── HOW_TO_USE.md      # This file
```

---

## Future Enhancements

- ✅ CSV export (completed)
- ⏳ SQL data lake integration (planned)
- ⏳ Automated scheduling (planned)
- ⏳ Email notifications (planned)

---

## Need Help?

If you encounter any issues:
1. Check the **Processing Log** in Tab 3
2. Review the **validation results** in Tab 1
3. Make sure your files have the correct format:
   - Row 1: Header with building/sensor info
   - Row 2: Column names (Date, Excel Time, Value, Notes)
   - Row 3+: Data

---

**Built for Fischer Energy Partners**
