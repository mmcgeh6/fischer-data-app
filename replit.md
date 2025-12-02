# Fischer Energy Partners - Data Processing Application

## Project Overview
This is a Streamlit-based web application for processing building management system (BMS) sensor data. It automates the process of combining multiple CSV/Excel files, resampling to 15-minute intervals, and flagging data quality issues.

**Current Version:** V11
**Primary File:** `src/app_v11.py`

## Recent Changes (2025-12-02)
- Imported GitHub repository to Replit
- Installed Python 3.11 and all required dependencies
- Configured Streamlit to run on port 5000 with proper Replit proxy settings
- Set up CLAUDE_API_KEY secret for AI-powered file analysis
- Configured deployment settings for autoscale deployment
- Created workflow to run the Streamlit application

## Project Architecture

### Technology Stack
- **Frontend:** Streamlit (Python web framework)
- **Data Processing:** Pandas for data manipulation
- **AI Integration:** Anthropic Claude API for automated file analysis
- **Visualization:** Plotly for charts
- **File Handling:** openpyxl for Excel file support

### Key Features
1. **AI-Powered File Analysis** - Automatically detects file structure, delimiters, and columns
2. **Timestamp Normalization** - Handles multiple date/time formats
3. **Data Merging** - Combines up to 90 files with different sensors
4. **Quarter-Hour Resampling** - Resamples minute-level data to 15-minute intervals
5. **Quality Flagging** - Detects stale data, zero values, and inexact matches
6. **Excel Output** - Generates color-coded Excel files with visual indicators
7. **File Archiving** - Automatically archives uploaded files by building name

### Core Components
- `src/app_v11.py` - Main Streamlit application (V11 - current)
- `src/timestamp_normalizer.py` - Robust date/time parsing module
- `src/data_processor.py` - Core data merging logic
- `assets/` - Fischer Energy branding assets (logo)
- `.streamlit/config.toml` - Streamlit configuration (theming, server settings)

### Legacy Files
- `src/app_v6.py` through `src/app_v10.py` - Previous versions (kept for reference)
- Various test files and documentation in root directory

## Environment Setup

### Required Secrets
- `CLAUDE_API_KEY` - API key for Anthropic Claude (required for AI file analysis)
  - Get from: https://console.anthropic.com/
  - Used for automatic detection of file structure

### Configuration Files
- `.streamlit/config.toml` - Streamlit server and theme configuration
  - Port: 5000
  - Host: 0.0.0.0
  - CORS disabled for Replit proxy compatibility

### Dependencies
All dependencies are listed in `requirements.txt`:
- pandas >= 2.0.0
- openpyxl >= 3.1.0
- streamlit >= 1.28.0
- plotly >= 5.17.0
- anthropic >= 0.18.0
- python-dotenv >= 1.0.0
- python-dateutil >= 2.8.2
- tzdata >= 2022.1

## Running the Application

### Development
The Streamlit workflow is already configured and runs automatically:
- Workflow: "Streamlit App"
- Command: `streamlit run src/app_v11.py`
- URL: Available via Replit webview on port 5000

### Deployment
Deployment is configured for autoscale mode:
- Type: Autoscale (stateless web application)
- Command: Streamlit with proper server configuration
- Port: 5000

## User Workflow (4 Steps in V11)

1. **Upload Files** - Enter building name and upload CSV/Excel sensor files
2. **AI Analysis** - Automatic detection of file structure and configuration
3. **Review Configuration** - Verify detected settings, edit if needed
4. **Process** - Single-button automatic processing (combine → resample → export)

The V11 version features a fully automatic workflow with progress tracking.

## File Organization

### Input
- Users upload files via Streamlit interface
- Files are automatically archived to `archive/[Building Name]/`

### Output
- Raw combined CSV (before resampling)
- Resampled Excel file with color-coded quality flags
- Both saved to archive folder with timestamps

### Data Quality Flags
- **Stale_Data_Flag** - Detects 3+ consecutive identical sensor readings
- **Zero_Value_Flag** - Tracks zero values (Clear/Single/Repeated)
- **Inexact Match** - Highlights values pulled from non-quarter-hour timestamps

## Documentation Files
- `README.md` - Project overview and quick start
- `HOW_TO_RUN_V9.md` - Detailed user guide for V9 (mostly applicable to V11)
- `CLAUDE.md` - Comprehensive technical documentation
- `Docs/Developer_Handoff_Summary.md` - Developer handoff notes

## Future Enhancements
- SQL data lake integration for long-term storage
- Performance optimization for large datasets (90 files × 600k rows)
- Additional data quality checks
- Multi-timezone support (currently hardcoded to America/New_York)

## Notes for Developers
- The app uses session state to maintain data between interactions
- AI analysis runs in parallel (5 concurrent workers) for efficiency
- Resampling uses `pd.merge_asof()` for per-sensor nearest-value matching
- Excel coloring is applied for visual quality indicators (yellow=inexact, red=stale)
