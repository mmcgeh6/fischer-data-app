import pandas as pd
import os

# Path to the CSV data folder
data_folder = "CSVdata"

# List all Excel files
files = [f for f in os.listdir(data_folder) if f.endswith('.xlsx')]

print(f"Found {len(files)} Excel files:\n")
for file in files:
    print(f"  - {file}")

print("\n" + "="*80 + "\n")

# Read and display the first sample file
sample_file = "CWP-30 VFD Output.xlsx"
file_path = os.path.join(data_folder, sample_file)

print(f"Reading sample file: {sample_file}\n")

# Try reading the file
try:
    df = pd.read_excel(file_path)
    print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n")
    print("Column names:")
    print(df.columns.tolist())
    print("\nFirst 10 rows:")
    print(df.head(10))
    print("\nData types:")
    print(df.dtypes)
except Exception as e:
    print(f"Error reading file: {e}")

print("\n" + "="*80 + "\n")

# Read and display the combined output file
combined_file = "combined_1_16_2025 10_56_47.065 AM EST.xlsx"
combined_path = os.path.join(data_folder, combined_file)

print(f"Reading combined output file: {combined_file}\n")

try:
    df_combined = pd.read_excel(combined_path)
    print(f"Shape: {df_combined.shape[0]} rows × {df_combined.shape[1]} columns\n")
    print("Column names:")
    print(df_combined.columns.tolist())
    print("\nFirst 10 rows:")
    print(df_combined.head(10))
    print("\nData types:")
    print(df_combined.dtypes)
except Exception as e:
    print(f"Error reading file: {e}")
