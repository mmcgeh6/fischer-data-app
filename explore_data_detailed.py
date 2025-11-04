import pandas as pd
import os

# Path to sample file
file_path = os.path.join("CSVdata", "CWP-30 VFD Output.xlsx")

print("Reading with header row detection...\n")

# Read without skipping rows to see the raw structure
df_raw = pd.read_excel(file_path, header=None)

print("Raw file structure (first 15 rows):")
print(df_raw.head(15))
print("\n" + "="*80 + "\n")

# Now read properly, skipping the first row (header info) and using row 2 as column names
df = pd.read_excel(file_path, header=1)

print(f"Properly parsed file:")
print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n")
print("Column names:")
print(df.columns.tolist())
print("\nFirst 15 rows:")
print(df.head(15))
print("\nLast 5 rows:")
print(df.tail(5))
print("\nData types:")
print(df.dtypes)
print("\nValue column statistics:")
print(df['Value'].describe())
