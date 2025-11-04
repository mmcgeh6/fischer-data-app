"""
Test for V7 Per-Sensor Quarter-Hour Resampling
Demonstrates independent matching for each sensor column
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))
from app_v7 import resample_to_quarter_hour

def test_per_sensor_matching():
    """
    Test that each sensor independently finds its nearest value.
    This is the key difference from the old merge_asof approach.
    """
    print("="*80)
    print("TEST: PER-SENSOR INDEPENDENT MATCHING")
    print("="*80)
    print()

    # Create test scenario:
    # Target time: 12:15:00
    # Sensor_A has exact match at 12:15:00
    # Sensor_B has closest match at 12:16:30 (within tolerance)
    # Sensor_C has NO value within ±2 min (should be NULL)

    base_time = datetime(2024, 7, 18, 12, 0, 0)

    test_data = {
        'Date': [
            base_time,                              # 12:00:00
            base_time + timedelta(minutes=15),      # 12:15:00 - EXACT MARK
            base_time + timedelta(minutes=16, seconds=30),  # 12:16:30 - WITHIN TOLERANCE
            base_time + timedelta(minutes=30),      # 12:30:00
            base_time + timedelta(minutes=48),      # 12:48:00 - OUTSIDE TOLERANCE for 12:45
        ],
        'Sensor_A': [100, 200, None, 300, None],    # Has value at 12:15 (exact)
        'Sensor_B': [50, None, 250, 350, None],     # Has value at 12:16:30 (near 12:15)
        'Sensor_C': [10, None, None, 310, 410],     # No value near 12:15 (should be NULL)
    }

    combined_df = pd.DataFrame(test_data)

    print("INPUT DATA:")
    print(combined_df.to_string(index=False))
    print()
    print("Expected behavior for 12:15:00 target:")
    print("  - Sensor_A: 200 (exact match at 12:15:00)")
    print("  - Sensor_B: 250 (from 12:16:30, within ±2min)")
    print("  - Sensor_C: NULL (no value within ±2min)")
    print()
    print("Expected behavior for 12:45:00 target:")
    print("  - Sensor_A: NULL (12:30 is outside tolerance)")
    print("  - Sensor_B: NULL (12:30 is outside tolerance)")
    print("  - Sensor_C: NULL (12:48 is outside tolerance)")
    print()
    print("-"*80)

    # Run resampling
    resampled_df, stats = resample_to_quarter_hour(combined_df, tolerance_minutes=2)

    print()
    print("RESAMPLED DATA:")
    print("="*80)

    # Show only the data columns first
    data_cols = ['Date', 'Sensor_A', 'Sensor_B', 'Sensor_C']
    print("\n[VALUE COLUMNS]")
    print(resampled_df[data_cols].to_string(index=False))

    # Show inexact flags
    inexact_cols = ['Date', 'Sensor_A_Inexact_Flag', 'Sensor_B_Inexact_Flag', 'Sensor_C_Inexact_Flag']
    print("\n[INEXACT MATCH FLAGS]")
    print(resampled_df[inexact_cols].to_string(index=False))

    # Show stale flags
    stale_cols = ['Date', 'Sensor_A_Stale_Flag', 'Sensor_B_Stale_Flag', 'Sensor_C_Stale_Flag']
    print("\n[STALE DATA FLAGS]")
    print(resampled_df[stale_cols].to_string(index=False))

    print()
    print("="*80)
    print("VERIFICATION:")
    print("="*80)

    # Verify 12:15:00 row
    row_1215 = resampled_df[resampled_df['Date'] == datetime(2024, 7, 18, 12, 15, 0)]
    if not row_1215.empty:
        print("\n✓ 12:15:00 Target:")
        print(f"  Sensor_A = {row_1215['Sensor_A'].iloc[0]} (expected: 200.0)")
        print(f"  Sensor_B = {row_1215['Sensor_B'].iloc[0]} (expected: 250.0)")
        print(f"  Sensor_C = {row_1215['Sensor_C'].iloc[0]} (expected: NaN)")
        print(f"  Sensor_A_Inexact_Flag = {row_1215['Sensor_A_Inexact_Flag'].iloc[0]} (expected: False)")
        print(f"  Sensor_B_Inexact_Flag = {row_1215['Sensor_B_Inexact_Flag'].iloc[0]} (expected: True)")

        # Assertions
        assert row_1215['Sensor_A'].iloc[0] == 200.0, "Sensor_A should be 200"
        assert row_1215['Sensor_B'].iloc[0] == 250.0, "Sensor_B should be 250"
        assert pd.isna(row_1215['Sensor_C'].iloc[0]), "Sensor_C should be NULL"
        assert row_1215['Sensor_A_Inexact_Flag'].iloc[0] == False, "Sensor_A should be exact"
        assert row_1215['Sensor_B_Inexact_Flag'].iloc[0] == True, "Sensor_B should be inexact"
        print("  ✅ All assertions passed!")

    # Verify 12:45:00 row (all should be NULL since 12:48 is outside tolerance)
    row_1245 = resampled_df[resampled_df['Date'] == datetime(2024, 7, 18, 12, 45, 0)]
    if not row_1245.empty:
        print("\n✓ 12:45:00 Target:")
        print(f"  Sensor_A = {row_1245['Sensor_A'].iloc[0]} (expected: NaN)")
        print(f"  Sensor_B = {row_1245['Sensor_B'].iloc[0]} (expected: NaN)")
        print(f"  Sensor_C = {row_1245['Sensor_C'].iloc[0]} (expected: NaN)")

        assert pd.isna(row_1245['Sensor_A'].iloc[0]), "Sensor_A should be NULL"
        assert pd.isna(row_1245['Sensor_B'].iloc[0]), "Sensor_B should be NULL"
        assert pd.isna(row_1245['Sensor_C'].iloc[0]), "Sensor_C should be NULL"
        print("  ✅ All assertions passed!")

    print()
    print("="*80)
    print("STATISTICS:")
    print("="*80)
    print(f"Total intervals: {stats['total_intervals']}")
    print(f"Total inexact matches: {stats['total_inexact_matches']}")
    print(f"Stale by sensor: {stats['stale_by_sensor']}")
    print(f"Total stale: {stats['total_stale']}")

    print()
    print("="*80)
    print("✅ TEST PASSED - Per-sensor matching works correctly!")
    print("="*80)


def test_zero_preservation():
    """Test that 0 values are preserved (not converted to NULL)."""
    print()
    print("="*80)
    print("TEST: ZERO VALUE PRESERVATION")
    print("="*80)
    print()

    base_time = datetime(2024, 7, 18, 12, 0, 0)

    test_data = {
        'Date': [
            base_time,
            base_time + timedelta(minutes=15),
        ],
        'Sensor_A': [0, 0],  # Zero values
        'Sensor_B': [100, 0],  # Mix of values
    }

    combined_df = pd.DataFrame(test_data)

    print("INPUT DATA:")
    print(combined_df.to_string(index=False))
    print()

    resampled_df, stats = resample_to_quarter_hour(combined_df)

    print("RESAMPLED DATA:")
    print(resampled_df[['Date', 'Sensor_A', 'Sensor_B']].to_string(index=False))
    print()

    # Verify zeros are preserved
    assert resampled_df['Sensor_A'].iloc[0] == 0, "Zero should be preserved"
    assert resampled_df['Sensor_A'].iloc[1] == 0, "Zero should be preserved"
    assert resampled_df['Sensor_B'].iloc[1] == 0, "Zero should be preserved"

    print("✅ TEST PASSED - Zero values preserved correctly!")
    print()


def test_stale_data_flagging():
    """Test that stale data (4+ consecutive identical) is flagged correctly."""
    print()
    print("="*80)
    print("TEST: STALE DATA FLAGGING")
    print("="*80)
    print()

    base_time = datetime(2024, 7, 18, 12, 0, 0)

    # Create 6 quarter-hour intervals with repeating values
    test_data = {
        'Date': [base_time + timedelta(minutes=15*i) for i in range(6)],
        'Sensor_A': [100, 100, 100, 100, 100, 105],  # 5 consecutive 100s
        'Sensor_B': [50, 51, 52, 53, 54, 55],  # All different
    }

    combined_df = pd.DataFrame(test_data)

    print("INPUT DATA:")
    print(combined_df.to_string(index=False))
    print()

    resampled_df, stats = resample_to_quarter_hour(combined_df)

    print("RESAMPLED DATA WITH FLAGS:")
    print(resampled_df[['Date', 'Sensor_A', 'Sensor_A_Stale_Flag', 'Sensor_B', 'Sensor_B_Stale_Flag']].to_string(index=False))
    print()

    # Sensor_A should have stale flags starting at row 3 (4th value)
    # because it equals the previous 3 values
    assert resampled_df['Sensor_A_Stale_Flag'].iloc[3] == True, "Row 3 should be flagged (4th consecutive)"
    assert resampled_df['Sensor_A_Stale_Flag'].iloc[4] == True, "Row 4 should be flagged (5th consecutive)"
    assert resampled_df['Sensor_A_Stale_Flag'].iloc[5] == False, "Row 5 should not be flagged (value changed)"

    # Sensor_B should have no stale flags
    assert resampled_df['Sensor_B_Stale_Flag'].sum() == 0, "Sensor_B should have no stale flags"

    print(f"Stale flags detected: {stats['stale_by_sensor']}")
    print("✅ TEST PASSED - Stale data flagged correctly!")
    print()


if __name__ == "__main__":
    try:
        test_per_sensor_matching()
        test_zero_preservation()
        test_stale_data_flagging()

        print()
        print("="*80)
        print("🎉 ALL TESTS PASSED!")
        print("="*80)

    except AssertionError as e:
        print()
        print("="*80)
        print(f"❌ TEST FAILED: {e}")
        print("="*80)
        sys.exit(1)
    except Exception as e:
        print()
        print("="*80)
        print(f"❌ ERROR: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
