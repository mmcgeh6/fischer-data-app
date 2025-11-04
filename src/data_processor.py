"""
Fischer Energy Partners - Data Processing Module

This module handles all the core data processing logic:
1. Reading raw sensor files
2. Cleaning and standardizing data
3. Combining multiple files
4. Resampling to 15-minute intervals
5. Flagging data quality issues
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Main class for processing building management system data.

    This class takes raw sensor export files and produces clean,
    combined data at 15-minute intervals with quality flags.
    """

    def __init__(self):
        """Initialize the processor with empty data storage."""
        self.raw_dataframes = []  # List to store each sensor's data
        self.combined_df = None   # The merged dataset
        self.resampled_df = None  # Final 15-minute interval data
        self.processing_log = []  # Track what happened during processing

    def log_message(self, message, level="INFO"):
        """Add a message to the processing log."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {level}: {message}"
        self.processing_log.append(log_entry)
        logger.info(message)

    def _read_file(self, file_path, header=None):
        """
        Helper function to read either CSV or Excel files.
        Uses flexible parsing for CSVs to handle various formats.

        Args:
            file_path: Path to the file
            header: Which row to use as header (None, 0, 1, etc.)

        Returns:
            pandas DataFrame
        """
        file_path = Path(file_path)

        if file_path.suffix.lower() in ['.csv', '.txt']:
            # Flexible CSV reading - try to auto-detect delimiter and handle errors
            return pd.read_csv(
                file_path,
                header=header,
                sep=None,  # Auto-detect separator (comma, tab, etc.)
                engine='python',  # More flexible parser
                encoding='utf-8',  # Try UTF-8 first
                encoding_errors='ignore',  # Skip encoding issues
                on_bad_lines='skip'  # Skip problematic lines instead of failing
            )
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            return pd.read_excel(file_path, header=header)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

    def scan_file(self, file_path):
        """
        Scan a file to check its structure and data quality.

        Returns a dictionary with validation results:
        - file_name: Name of the file
        - has_expected_columns: Boolean
        - date_format_detected: String describing the format
        - value_column_quality: Percentage of numeric values
        - row_count: Number of data rows
        - sample_rows: First few rows for preview
        """
        try:
            # Read the file (supports both CSV and Excel)
            df_raw = self._read_file(file_path, header=None)

            # Use filename as sensor name (simple and reliable)
            sensor_name = Path(file_path).stem

            # Auto-detect header row
            if 'Date' in str(df_raw.iloc[0].values):
                df = self._read_file(file_path, header=0)
            elif len(df_raw) > 1 and 'Date' in str(df_raw.iloc[1].values):
                df = self._read_file(file_path, header=1)
            else:
                df = self._read_file(file_path, header=0)

            # Check for expected columns
            expected_cols = ['Date', 'Value']
            has_expected = all(col in df.columns for col in expected_cols)

            # Analyze value column
            value_numeric_pct = 0
            if 'Value' in df.columns:
                # Try to convert to numeric and see how many succeed
                numeric_values = pd.to_numeric(df['Value'], errors='coerce')
                value_numeric_pct = (numeric_values.notna().sum() / len(df)) * 100

            # Sample date format
            date_sample = df['Date'].iloc[0] if 'Date' in df.columns and len(df) > 0 else "N/A"

            return {
                'file_name': Path(file_path).name,
                'sensor_name': sensor_name,
                'has_expected_columns': has_expected,
                'columns_found': list(df.columns),
                'date_sample': str(date_sample),
                'value_numeric_pct': round(value_numeric_pct, 2),
                'row_count': len(df),
                'sample_rows': df.head(3).to_dict('records'),
                'status': 'OK' if has_expected and value_numeric_pct > 95 else 'WARNING'
            }

        except Exception as e:
            return {
                'file_name': Path(file_path).name,
                'status': 'ERROR',
                'error_message': str(e)
            }

    def load_file(self, file_path):
        """
        Load a single sensor data file and prepare it for merging.

        Steps:
        1. Read the Excel file, skipping the header row
        2. Extract the sensor name from the first row or filename
        3. Parse dates into a standard format
        4. Keep only Date, Excel Time, and Value columns
        5. Rename 'Value' column to the sensor name

        Returns: pandas DataFrame or None if error
        """
        try:
            self.log_message(f"Loading file: {Path(file_path).name}")

            # Simple approach: Use filename as sensor name
            sensor_name = Path(file_path).stem

            # Try to auto-detect the header row
            # Read without header first to inspect
            df_raw = self._read_file(file_path, header=None)

            # Check if row 0 contains "Date" - if so, it's the header
            if 'Date' in str(df_raw.iloc[0].values):
                # Row 0 is the header
                df = self._read_file(file_path, header=0)
            # Check if row 1 contains "Date" - row 0 might be metadata
            elif len(df_raw) > 1 and 'Date' in str(df_raw.iloc[1].values):
                # Row 1 is the header, row 0 is metadata
                df = self._read_file(file_path, header=1)
            else:
                # Fallback: assume row 0 is header
                df = self._read_file(file_path, header=0)

            self.log_message(f"  Sensor name: {sensor_name}")

            # Validate required columns exist
            if 'Date' not in df.columns or 'Value' not in df.columns:
                self.log_message(f"ERROR: Missing required columns in {Path(file_path).name}", "ERROR")
                return None

            # Parse the Date column
            # The format appears to be: "7/18/2024 12:00:00 PM EDT"
            df['Date'] = pd.to_datetime(df['Date'], format='mixed', errors='coerce')

            # Remove timezone info to simplify merging (we'll add it back later if needed)
            df['Date'] = df['Date'].dt.tz_localize(None)

            # Keep only the columns we need - Date and Value are essential
            columns_to_keep = ['Date', 'Value']
            df = df[columns_to_keep]

            # Rename 'Value' to the sensor name
            df = df.rename(columns={'Value': sensor_name})

            # Remove any rows where Date couldn't be parsed
            df = df.dropna(subset=['Date'])

            self.log_message(f"Successfully loaded {len(df)} rows from {sensor_name}")

            return df

        except Exception as e:
            self.log_message(f"ERROR loading {Path(file_path).name}: {str(e)}", "ERROR")
            return None

    def load_multiple_files(self, file_paths):
        """
        Load multiple sensor files.

        Args:
            file_paths: List of file paths to load

        Returns:
            List of successfully loaded DataFrames
        """
        self.raw_dataframes = []

        for file_path in file_paths:
            df = self.load_file(file_path)
            if df is not None:
                self.raw_dataframes.append(df)

        self.log_message(f"Successfully loaded {len(self.raw_dataframes)} out of {len(file_paths)} files")
        return self.raw_dataframes

    def combine_files(self):
        """
        Combine all loaded sensor files into a single DataFrame.

        Uses an outer join on the 'Date' column to ensure all timestamps
        are preserved, even if not all sensors have data at that time.

        Returns: Combined DataFrame
        """
        if not self.raw_dataframes:
            self.log_message("ERROR: No data files loaded", "ERROR")
            return None

        self.log_message("Combining all sensor files...")

        # Start with the first dataframe
        combined = self.raw_dataframes[0].copy()

        # Merge each subsequent dataframe
        # IMPORTANT: Always merge on Date (timestamp) only
        # This ensures values are matched by their timestamp
        for df in self.raw_dataframes[1:]:
            combined = pd.merge(
                combined,
                df,
                on='Date',
                how='outer'
            )

        # Sort by date
        combined = combined.sort_values('Date').reset_index(drop=True)

        self.combined_df = combined
        self.log_message(f"Combined data: {len(combined)} rows × {len(combined.columns)} columns")

        return combined

    def resample_to_15min(self, tolerance_minutes=2):
        """
        Resample the data to 15-minute intervals with PER-SENSOR nearest-value matching.

        Each sensor independently finds its closest value within ±2 minutes of each quarter-hour mark.
        This allows different sensors to pull from different source timestamps for the same target time.

        Args:
            tolerance_minutes: How far (in minutes) from the 15-min mark to search

        Returns: Resampled DataFrame with per-sensor quality flags
        """
        if self.combined_df is None:
            self.log_message("ERROR: No combined data to resample", "ERROR")
            return None

        self.log_message(f"Resampling to 15-minute intervals (tolerance: ±{tolerance_minutes} min)...")

        # Create a complete range of 15-minute timestamps
        start_time = self.combined_df['Date'].min()
        end_time = self.combined_df['Date'].max()

        # Round start to previous 15-min mark
        start_time = start_time.replace(minute=(start_time.minute // 15) * 15, second=0, microsecond=0)

        # Round end to next 15-min mark
        end_minute = ((end_time.minute // 15) + 1) * 15
        if end_minute >= 60:
            end_time = end_time + timedelta(hours=1)
            end_time = end_time.replace(minute=0, second=0, microsecond=0)
        else:
            end_time = end_time.replace(minute=end_minute, second=0, microsecond=0)

        # Generate 15-minute interval timestamps
        target_timestamps = pd.date_range(start=start_time, end=end_time, freq='15min')

        # Get sensor columns (exclude Date)
        sensor_cols = [col for col in self.combined_df.columns if col != 'Date']

        # Initialize result lists
        result_data = {'Date': target_timestamps}
        for sensor in sensor_cols:
            result_data[sensor] = []
            result_data[f'{sensor}_Inexact_Flag'] = []

        tolerance = pd.Timedelta(minutes=tolerance_minutes)

        # For each target timestamp, find nearest value for each sensor independently
        for target_time in target_timestamps:
            # Calculate time differences for all rows
            time_diffs = (self.combined_df['Date'] - target_time).abs()
            within_window = time_diffs <= tolerance

            # For each sensor, find its nearest value
            for sensor in sensor_cols:
                # Filter to rows within tolerance that have a non-null value for this sensor
                valid_mask = within_window & self.combined_df[sensor].notna()

                if not valid_mask.any():
                    # No value within window - use NULL
                    result_data[sensor].append(None)
                    result_data[f'{sensor}_Inexact_Flag'].append(False)
                else:
                    # Find the row with the smallest time difference that has this sensor's value
                    valid_time_diffs = time_diffs[valid_mask]
                    closest_idx = valid_time_diffs.idxmin()

                    # Extract the value (preserve 0 as 0, not NULL)
                    value = self.combined_df.loc[closest_idx, sensor]
                    result_data[sensor].append(value)

                    # Check if the source timestamp is exactly on the quarter-hour mark
                    source_time = self.combined_df.loc[closest_idx, 'Date']
                    is_exact = (source_time.minute % 15 == 0) and (source_time.second == 0)
                    result_data[f'{sensor}_Inexact_Flag'].append(not is_exact)

        # Create DataFrame
        resampled = pd.DataFrame(result_data)

        # Count inexact matches
        inexact_flag_cols = [col for col in resampled.columns if col.endswith('_Inexact_Flag')]
        total_inexact = sum(resampled[col].sum() for col in inexact_flag_cols)

        self.log_message(f"Resampled to {len(resampled)} 15-minute intervals")
        self.log_message(f"Total inexact matches across all sensors: {int(total_inexact)}")

        self.resampled_df = resampled
        return resampled

    def flag_stale_data(self, consecutive_repeats=4):
        """
        Flag sensor values that repeat for more than 3 consecutive intervals.

        This indicates potentially "stuck" or non-functioning sensors.

        Args:
            consecutive_repeats: Number of repeats to trigger a flag (default 4 = current + 3 prior)

        Returns: DataFrame with added stale data flags
        """
        if self.resampled_df is None:
            self.log_message("ERROR: No resampled data to check for stale values", "ERROR")
            return None

        self.log_message(f"Flagging stale data (>{consecutive_repeats-1} consecutive repeats)...")

        df = self.resampled_df.copy()

        # Get all sensor columns (exclude Date and flag columns)
        sensor_cols = [col for col in df.columns
                      if col != 'Date'
                      and not col.endswith('_Inexact_Flag')
                      and not col.endswith('_Stale_Flag')]

        # For each sensor column, check for repeated values
        for col in sensor_cols:
            # Create a flag column for this sensor
            flag_col = f'{col}_Stale_Flag'

            # Check if current value equals the previous 3 values
            # (i.e., 4 consecutive identical values)
            is_repeated = (
                (df[col] == df[col].shift(1)) &
                (df[col] == df[col].shift(2)) &
                (df[col] == df[col].shift(3))
            )

            df[flag_col] = is_repeated

        # Count total stale flags
        stale_flag_cols = [col for col in df.columns if col.endswith('_Stale_Flag')]
        total_stale = df[stale_flag_cols].sum().sum()

        self.log_message(f"Found {int(total_stale)} stale data points across all sensors")

        self.resampled_df = df
        return df

    def export_to_csv(self, output_path):
        """
        Export the final processed data to a CSV file.

        Args:
            output_path: Where to save the CSV file

        Returns: True if successful, False otherwise
        """
        if self.resampled_df is None:
            self.log_message("ERROR: No processed data to export", "ERROR")
            return False

        try:
            # Make sure output directory exists
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Export to CSV
            self.resampled_df.to_csv(output_path, index=False)

            self.log_message(f"Successfully exported data to {output_path}")
            return True

        except Exception as e:
            self.log_message(f"ERROR exporting to CSV: {str(e)}", "ERROR")
            return False

    def save_minute_data_csv(self, output_path):
        """
        Save the minute-by-minute combined data (for future data lake).

        This is the intermediate data before resampling.
        In the future, this would go to a SQL database instead.

        Args:
            output_path: Where to save the CSV file

        Returns: True if successful, False otherwise
        """
        if self.combined_df is None:
            self.log_message("ERROR: No combined data to save", "ERROR")
            return False

        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            self.combined_df.to_csv(output_path, index=False)

            self.log_message(f"Saved minute-level data to {output_path} (future: SQL data lake)")
            return True

        except Exception as e:
            self.log_message(f"ERROR saving minute data: {str(e)}", "ERROR")
            return False

    def get_processing_summary(self):
        """
        Generate a summary of the processing results.

        Returns: Dictionary with key statistics
        """
        if self.resampled_df is None:
            return {"status": "No data processed"}

        df = self.resampled_df

        # Count sensor columns (exclude Date and all flag columns)
        sensor_cols = [col for col in df.columns
                      if col != 'Date'
                      and not col.endswith('_Inexact_Flag')
                      and not col.endswith('_Stale_Flag')]

        # Count inexact flags (per sensor)
        inexact_flag_cols = [col for col in df.columns if col.endswith('_Inexact_Flag')]
        inexact_count = sum(df[col].sum() for col in inexact_flag_cols) if inexact_flag_cols else 0

        # Count stale flags
        stale_flag_cols = [col for col in df.columns if col.endswith('_Stale_Flag')]
        stale_count = df[stale_flag_cols].sum().sum() if stale_flag_cols else 0

        return {
            'total_rows': len(df),
            'num_sensors': len(sensor_cols),
            'sensor_names': sensor_cols,
            'date_range': f"{df['Date'].min()} to {df['Date'].max()}",
            'inexact_matches': int(inexact_count),
            'stale_data_points': int(stale_count),
            'processing_log': self.processing_log
        }
