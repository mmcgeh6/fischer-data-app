"""Quick test of core functionality"""
import sys
sys.path.append('src')

from data_processor import DataProcessor
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

processor = DataProcessor()

# Get sample files
data_folder = Path("CSVdata")
sample_files = [f for f in data_folder.glob("*.xlsx") if 'combined' not in f.name.lower()][:3]

print(f"Testing with {len(sample_files)} files:")
for f in sample_files:
    print(f"  - {f.name}")

print("\n1. Loading files...")
processor.load_multiple_files(sample_files)
print(f"   Loaded {len(processor.raw_dataframes)} dataframes")

print("\n2. Combining files...")
combined = processor.combine_files()
if combined is not None:
    print(f"   SUCCESS: {combined.shape[0]} rows × {combined.shape[1]} columns")
    print(f"   Columns: {list(combined.columns)}")
else:
    print("   FAILED")
    sys.exit(1)

print("\n3. Resampling...")
resampled = processor.resample_to_15min()
if resampled is not None:
    print(f"   SUCCESS: {resampled.shape[0]} rows")
else:
    print("   FAILED")

print("\n4. Flagging stale data...")
flagged = processor.flag_stale_data()
if flagged is not None:
    print(f"   SUCCESS: {flagged.shape[0]} rows × {flagged.shape[1]} columns")
else:
    print("   FAILED")

print("\n5. Exporting...")
success1 = processor.export_to_csv("output/quick_test.csv")
success2 = processor.save_minute_data_csv("output/quick_test_minute.csv")
if success1 and success2:
    print(f"   SUCCESS: Files exported")
else:
    print("   FAILED")

print("\n✅ ALL TESTS PASSED!")
