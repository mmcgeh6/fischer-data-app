"""
Quick test of V7 quarter-hour resampling logic
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))
from app_v7 import resample_to_quarter_hour

def test_resampling():
    """Test the quarter-hour resampling function."""
    print("Testing V7 quarter-hour resampling...\n")

    # Create test data with various timestamps
    base_time = datetime(2024, 7, 18, 12, 0, 0)
    test_data = {
        'Date': [
            base_time,                           # Exactly on :00
            base_time + timedelta(minutes=1),    # :01 (should match :00)
            base_time + timedelta(minutes=14),   # :14 (should match :15)
            base_time + timedelta(minutes=15),   # Exactly on :15
            base_time + timedelta(minutes=16),   # :16 (should match :15)
            base_time + timedelta(minutes=30),   # Exactly on :30
            base_time + timedelta(minutes=31, seconds=5),  # :31:05 (should match :30)
            base_time + timedelta(minutes=44),   # :44 (should match :45)
            base_time + timedelta(minutes=45),   # Exactly on :45
            base_time + timedelta(minutes=60),   # Next hour :00
        ],
        'Sensor_A': [100, 100, 100, 100, 105, 110, 110, 110, 110, 115],  # Some stale values
        'Sensor_B': [50, 51, 52, 53, 54, 55, 56, 57, 58, 59]
    }

    combined_df = pd.DataFrame(test_data)
    print("Input Data:")
    print(combined_df)
    print(f"\nInput rows: {len(combined_df)}")
    print(f"Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")

    # Run resampling
    resampled_df, stats = resample_to_quarter_hour(combined_df)

    print("\n" + "="*60)
    print("RESAMPLED DATA:")
    print("="*60)
    print(resampled_df)

    print("\n" + "="*60)
    print("STATISTICS:")
    print("="*60)
    print(f"Total intervals: {stats['total_intervals']}")
    print(f"Inexact matches: {stats['inexact_matches']} ({stats['inexact_pct']}%)")
    print(f"Total stale flags: {stats['total_stale']}")
    print(f"\nStale by sensor:")
    for sensor, count in stats['stale_by_sensor'].items():
        print(f"  {sensor}: {count}")

    # Verify quality flags
    print("\n" + "="*60)
    print("QUALITY FLAG VERIFICATION:")
    print("="*60)

    # Check inexact matches
    inexact_rows = resampled_df[resampled_df['Inexact_Match_Flag'] == True]
    print(f"\nRows with Inexact_Match_Flag=True: {len(inexact_rows)}")
    if len(inexact_rows) > 0:
        print("These rows should have timestamps not on 15-min marks:")
        print(inexact_rows[['Date', 'Inexact_Match_Flag']])

    # Check stale flags for Sensor_A
    if 'Sensor_A_Stale_Flag' in resampled_df.columns:
        stale_a = resampled_df[resampled_df['Sensor_A_Stale_Flag'] == True]
        print(f"\nSensor_A stale flags: {len(stale_a)}")
        print("(Should flag rows where 4+ consecutive values are identical)")
        if len(stale_a) > 0:
            print(stale_a[['Date', 'Sensor_A', 'Sensor_A_Stale_Flag']])

    print("\n" + "="*60)
    print("TEST COMPLETE ✅")
    print("="*60)

if __name__ == "__main__":
    test_resampling()
