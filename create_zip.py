import zipfile
import os
from pathlib import Path

# Files and folders to include
items_to_zip = [
    'src',
    'requirements.txt',
    '.env',
    'README.md',
    'CLAUDE.md',
    'HOW_TO_RUN_V7.md',
    'HOW_TO_RUN_V6.md',
    'HOW_TO_USE.md',
    'HOW_TO_RUN.md',
    'test_v7_resampling.py',
    'demo_ai_integration.py',
    'explore_data.py',
    'explore_data_detailed.py',
    'quick_test.py',
    'requirements-MarcusPC.txt'
]

output_zip = 'fischer-app-clean.zip'

print(f"Creating {output_zip}...")
print(f"Working directory: {os.getcwd()}")

with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for item in items_to_zip:
        if os.path.exists(item):
            if os.path.isfile(item):
                print(f"  Adding file: {item}")
                zipf.write(item)
            elif os.path.isdir(item):
                print(f"  Adding directory: {item}/")
                for root, dirs, files in os.walk(item):
                    # Skip __pycache__ directories
                    dirs[:] = [d for d in dirs if d != '__pycache__']

                    for file in files:
                        if not file.endswith('.pyc'):
                            file_path = os.path.join(root, file)
                            arcname = file_path
                            print(f"    Adding: {file_path}")
                            zipf.write(file_path, arcname)
        else:
            print(f"  SKIPPED (not found): {item}")

print(f"\n✓ Created {output_zip}")
print(f"  File size: {os.path.getsize(output_zip) / 1024:.1f} KB")

# Count items in zip
with zipfile.ZipFile(output_zip, 'r') as zipf:
    print(f"  Total items: {len(zipf.namelist())}")
