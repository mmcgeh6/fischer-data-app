"""
Demo script to showcase the AI integration functionality
Run with: streamlit run demo_ai_integration.py
"""

import streamlit as st
import sys
import os

# Add src to path
sys.path.insert(0, 'src')

# Import the main app
from app_v5 import main

# Create demo instructions
st.sidebar.title("🤖 AI Integration Demo")
st.sidebar.markdown("""
## How to use the AI Auto-Detection:

1. **Upload Files**: Upload your Excel/CSV sensor files
2. **Click Auto-Detect**: For each file, click the "🤖 Auto-Detect" button
3. **AI Analysis**: Claude AI will analyze the file and automatically:
   - Detect header rows
   - Find date, time, and value columns
   - Extract sensor names
   - Configure delimiters
4. **Review & Adjust**: The AI settings are applied but you can still manually adjust if needed
5. **Combine & Export**: Process all files and export the combined data

## Benefits:
- ⚡ **Fast**: Seconds instead of minutes per file
- 🎯 **Accurate**: AI understands various file formats
- 🔄 **Flexible**: Manual override always available
- 📊 **Smart**: Learns from file structure and metadata

## Requirements:
- Claude API key in .env file ✅
- Python packages installed ✅
""")

# Run the main app
if __name__ == "__main__":
    main()