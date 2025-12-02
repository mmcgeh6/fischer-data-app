# Fischer Data Processing Application - Developer Handoff Summary

## Executive Overview

**Client:** Fischer Energy Partners (NYC-based energy consultation firm)
**Purpose:** Automate the manual process of combining, cleaning, and resampling building management system (BMS) sensor data exports
**Current State:** Functional MVP (Version 8) with Streamlit UI, AI-powered file analysis, and Excel output with visual quality indicators
**Next Phase:** Code optimization, UI/UX enhancement, performance tuning, and SQL data lake integration

---

## Business Problem

Fischer Energy Partners receives **10-90 CSV/Excel files per building** from BMS exports containing minute-by-minute sensor readings. Their current manual process involves:

1. Opening each file individually in Excel
2. Manually identifying and copying the correct columns (Date, Value)
3. Handling inconsistent date formats across different building systems
4. Combining all sensors into a single spreadsheet by timestamp
5. Deduplicating overlapping date ranges from different sensors
6. Resampling from minute-level data to **15-minute intervals** (quarter-hour marks: :00, :15, :30, :45)
7. Flagging data quality issues (inexact matches, stale sensor readings)

**Pain Points:**
- Extremely time-consuming (hours per building)
- Error-prone manual data handling
- Inconsistent date format handling
- No systematic quality control
- Cannot scale to multiple buildings efficiently

**Target Performance:** Handle 10-90 files × 100-600,000 rows each in minutes, not hours.

---

## Application Architecture (Current MVP - V8)

### Technology Stack
- **Frontend:** Streamlit (Python web framework)
- **Data Processing:** Pandas (optimized C-backed operations)
- **AI Integration:** Anthropic Claude Sonnet 4.5 (automated file analysis)
- **Output:** Excel files with color-coded quality indicators (.xlsx format via openpyxl)
- **Future:** SQL database integration for data lake storage

### Core Components

**1. AI-Powered File Analysis** (`src/app_v8.py`, lines 95-200)
- Analyzes first 15 lines of each uploaded file in parallel (5 concurrent workers)
- Automatically detects: delimiter, header row location, date column, value column, sensor name
- Typical response time: 1-3 seconds per file
- User can review and edit AI suggestions before processing

**2. Timestamp Normalization** (`src/timestamp_normalizer.py`)
- Handles multiple input formats: US-style 12-hour, 24-hour, ISO-style, text month formats
- Supports timezone abbreviations (EST, EDT, CST, etc.)
- Standardizes all output to `MM/DD/YYYY HH:MM:SS` (24-hour format)
- Robust error handling with pandas fallback

**3. Data Merging & Deduplication** (`src/data_processor.py`)
- Outer join on Date column across all sensor files
- Each sensor becomes a separate column in combined dataset
- Automatic duplicate row removal
- Preserves minute-level raw data for optional download

**4. Quarter-Hour Resampling Algorithm**
- **Key Innovation:** Per-sensor independent matching using `pd.merge_asof()`
- Creates complete 15-minute timestamp grid across entire date range
- Each sensor finds its own nearest value within ±2 minute tolerance
- Efficient O(n log n) performance per sensor
- Tracks which cells are inexact for visual highlighting

**5. Quality Flagging System**
- **Inexact Match Detection:** Values pulled from non-quarter-hour timestamps
- **Stale Data Detection:** 4+ consecutive identical readings per sensor
- V8 consolidates flags: single `Stale_Data_Flag` column + `Stale_Sensors` list (cleaner than V7's per-sensor columns)

**6. Excel Output with Visual Indicators** (V8 Feature)
- **Yellow cells:** Inexact matches (interpolated data)
- **Light red rows:** Stale sensor data detected
- Clean, readable format for stakeholder reports
- Maintains Excel compatibility for filtering/analysis

### 6-Step User Workflow
1. **Upload Files:** Drag-and-drop CSV/Excel files into Streamlit interface
2. **AI Analysis:** Click "Analyze All Files" - parallel batch processing
3. **Review/Edit:** Verify AI-detected configurations, see extracted data preview
4. **Combine:** Merge all sensors, normalize timestamps, download optional raw CSV
5. **Resample:** Generate 15-minute intervals with per-sensor matching
6. **Export Excel:** Download color-coded .xlsx file with quality indicators

---

## Original Plan vs. Current Implementation

### What Changed From Original SQL Plan

**Original Vision (Docs/Fischer Sql Markdown Plan.md):**
- SQL-first approach with stored procedures for data transformation
- Data lake for minute-level storage as **priority #1**
- CSV export as **priority #2**

**Current Implementation:**
- Python-first approach (Pandas) for complex transformations
- CSV/Excel export as **priority #1** (immediate business value)
- SQL data lake integration as **priority #2** (deferred for future phase)

**Rationale for Changes:**
- **Timestamp operations:** `pd.merge_asof()` in Pandas is far more efficient and maintainable than SQL window functions for nearest-value matching
- **Inconsistent file formats:** Python handles dynamic CSV parsing, multiple encodings, and varying structures better than SQL bulk inserts
- **Iterative development:** GUI-first approach allowed rapid user feedback and validation
- **Excel Time column:** Initially thought critical, later discovered to be a data export artifact and removed entirely in V6

### What Stayed the Same
- Core business logic: combine → deduplicate → resample → flag
- ±2 minute tolerance window for quarter-hour matching
- Stale data definition: 4+ consecutive identical values
- Outer join strategy for handling overlapping sensor timestamps
- Future-proofing for timezone expansion beyond NYC (currently hardcoded to America/New_York)

---

## Development Next Steps & Priorities

### Phase 1: Optimization & Hardening (High Priority)
1. **Performance Profiling**
   - Benchmark with full production datasets (90 files × 600k rows)
   - Identify bottlenecks in timestamp normalization and merge operations
   - Optimize memory usage for large datasets (target: <4GB RAM for 54M rows)

2. **Error Handling & Validation**
   - Add pre-flight file validation: check expected columns before processing
   - Handle corrupted/non-numeric values in Value column (NULL, #ERROR, text)
   - Improve date format detection for edge cases (European formats, ISO 8601)
   - Generate detailed processing logs and error reports

3. **Code Quality**
   - Refactor monolithic `app_v8.py` (~900+ lines) into modular components
   - Add comprehensive unit tests for core functions
   - Document function parameters and return types
   - Remove legacy code from V1-V7 versions (currently kept for reference)

### Phase 2: UI/UX Enhancement (Medium Priority)
1. **Interactive Pre-Flight Dashboard**
   - Show file health checks: column detection confidence, data quality snapshot
   - Preview date format parsing with sample conversions
   - Flag files with parsing issues before processing
   - Add "Exclude File" option for problematic files

2. **Visual Improvements**
   - Progress bars for each processing step (not just AI analysis)
   - Data quality summary charts (inexact match %, stale sensor counts)
   - Interactive resampling preview before final export
   - Responsive design for different screen sizes

3. **User Experience**
   - Configurable tolerance window (currently hardcoded ±2 minutes)
   - Adjustable stale threshold (currently hardcoded 4 consecutive values)
   - Batch processing: save/load file configurations for repeated use
   - Export settings: customize Excel colors, choose output columns

### Phase 3: SQL Data Lake Integration (Future Phase)
**Original requirement preserved from initial plan:**

1. **Database Schema Design**
   - Minute-level raw data table (long-term storage)
   - Metadata table: building ID, sensor names, file processing timestamps
   - Resampled data table (15-minute intervals for fast querying)

2. **ETL Pipeline**
   - Python processes files → writes raw data to SQL (bulk insert)
   - Separate resampling query/view for 15-minute interval access
   - Schedule: automated folder watching for new BMS exports

3. **Technology Choices** (TBD - Need discussion)
   - PostgreSQL vs. SQL Server vs. MySQL
   - Local database vs. cloud (Azure SQL, AWS RDS)
   - ORM choice: SQLAlchemy or direct connections

4. **Hybrid Workflow**
   - **Option A:** Process → CSV export → manual SQL import
   - **Option B:** Process → direct SQL write → CSV export from database
   - **Option C:** Process → dual output (CSV + SQL simultaneously)

---

## Known Issues & Technical Debt

1. **Session State Management:** Streamlit session state can become stale during long sessions (minor UX issue)
2. **Excel Color Limitations:** Very large files (>500k rows) may have slow color application
3. **AI API Cost:** Each file analysis costs ~$0.01-0.02 per API call (acceptable for current volume)
4. **Windows Path Handling:** Some hardcoded path separators may fail on Linux/Mac
5. **No Authentication:** Application has no user login system (fine for local use, problematic for web deployment)
6. **Single Timezone:** Currently hardcoded to America/New_York (future-proof design exists but not exposed to UI)

---

## Key Discussion Topics for Developer Meeting

1. **SQL Integration Strategy:** Which database, when to integrate, dual-output approach?
2. **Deployment Model:** Local desktop app vs. internal web server vs. cloud hosting?
3. **Performance Targets:** What's acceptable processing time for 90 files × 600k rows?
4. **UI Framework:** Keep Streamlit (rapid development) or migrate to React/Flask (more control)?
5. **Testing Strategy:** Automated testing approach with real building data samples
6. **Version Control:** Clean up V1-V7 legacy code or keep for historical reference?
7. **Feature Priorities:** Which Phase 2 enhancements deliver most business value first?

---

## Repository Structure & Key Files

```
Fischer Data App V1/
├── src/
│   ├── app_v8.py                  # Current production application
│   ├── timestamp_normalizer.py    # Robust date parsing module
│   ├── data_processor.py          # Core data merging logic
│   └── app_v1-v7.py              # Legacy versions (historical reference)
├── CSVdata/                       # Sample input files (100 rows each)
├── output/                        # Generated output files
├── Docs/
│   ├── Fischer Sql Markdown Plan.md      # Original requirements discussion
│   └── Developer_Handoff_Summary.md      # This document
├── requirements.txt               # Python dependencies
├── CLAUDE.md                      # Detailed technical documentation
├── HOW_TO_RUN_V8.md              # User guide for current version
└── .env                          # API key configuration (not in git)
```

**Essential Reading Before Development:**
1. This document (overview)
2. `CLAUDE.md` (comprehensive technical documentation)
3. `HOW_TO_RUN_V8.md` (current feature set and workflow)
4. `Docs/Fischer Sql Markdown Plan.md` (original requirements and design rationale)

---

## Success Metrics & Definition of Done

**MVP Complete (Current State):**
- ✅ Handles sample datasets (12 files × 100 rows)
- ✅ AI-powered automation reduces manual steps by 90%
- ✅ Timestamp normalization works for known formats
- ✅ Quarter-hour resampling with quality flags functional
- ✅ Excel output with visual indicators

**Production Ready (Target State):**
- ⬜ Handles production scale (90 files × 600k rows in <5 minutes)
- ⬜ Graceful error handling for all known edge cases
- ⬜ Comprehensive test coverage (unit + integration tests)
- ⬜ User documentation and training materials
- ⬜ Optimized UI/UX based on Fischer team feedback
- ⬜ SQL data lake integration operational
- ⬜ Automated regression testing with real building data

---

**Document Version:** 1.0
**Last Updated:** 2025-11-04
**Prepared For:** Developer handoff meeting with Fischer Energy Partners team
