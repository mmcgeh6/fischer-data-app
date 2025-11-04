# Fischer Energy Partners - Data Processing Application

## Overview
This application processes building management system (BMS) data exports from multiple CSV/Excel files, combines them, resamples to 15-minute intervals, and flags data quality issues.

## Project Structure
```
Fischer Data App V1/
├── CSVdata/              # Input data files (raw sensor exports)
├── src/                  # Source code
├── output/               # Generated clean CSV files
├── logs/                 # Processing logs
├── Docs/                 # Documentation
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

## Setup Instructions (Beginner-Friendly)

### Step 1: Install Required Packages
Open a terminal/command prompt in this folder and run:
```bash
pip install -r requirements.txt
```

This installs:
- **pandas**: For data processing
- **openpyxl**: For reading Excel files
- **streamlit**: For the web-based GUI
- **plotly**: For data visualization

### Step 2: Run the Application
```bash
streamlit run src/app.py
```

This will open a web browser with the application interface.

## How It Works

1. **Upload Files**: Select your raw sensor data files
2. **Validation**: App scans files and shows a preview
3. **Process**: Combines, cleans, and resamples data
4. **Export**: Download the clean CSV file

## Features
- ✅ Handles multiple date formats
- ✅ Combines up to 90 files automatically
- ✅ Resamples to 15-minute intervals
- ✅ Flags inexact time matches
- ✅ Flags stale/repeated sensor values
- ✅ Exports clean CSV ready for analysis
- ⏳ SQL data lake integration (coming later)
