"""
Fischer Energy Partners - Data Processing Application
A Streamlit web interface for processing building management system data
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Import our data processor
from data_processor import DataProcessor

# Page configuration
st.set_page_config(
    page_title="Fischer Data Processing",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'processor' not in st.session_state:
    st.session_state.processor = DataProcessor()
if 'files_uploaded' not in st.session_state:
    st.session_state.files_uploaded = False
if 'scan_results' not in st.session_state:
    st.session_state.scan_results = None
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False


def main():
    """Main application interface."""

    # Header
    st.title("📊 Fischer Energy Partners - Data Processing Tool")
    st.markdown("---")

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Settings")
        tolerance_minutes = st.slider(
            "Time Tolerance (minutes)",
            min_value=1,
            max_value=5,
            value=2,
            help="How far from the 15-minute mark to search for data (±minutes)"
        )

        consecutive_repeats = st.slider(
            "Stale Data Threshold",
            min_value=3,
            max_value=10,
            value=4,
            help="Flag data that repeats this many times consecutively"
        )

        st.markdown("---")
        st.markdown("### 📖 Quick Guide")
        st.markdown("""
        1. **Upload** your sensor data files
        2. **Review** the validation scan
        3. **Process** to combine and clean
        4. **Download** your clean CSV
        """)

    # Main content area - tabs for different steps
    tab1, tab2, tab3 = st.tabs(["1️⃣ Upload Files", "2️⃣ Process Data", "3️⃣ Results & Export"])

    # ========== TAB 1: FILE UPLOAD ==========
    with tab1:
        st.header("Upload Sensor Data Files")

        st.info("📁 Upload your raw sensor export files (.xlsx or .csv format)")

        # File uploader
        uploaded_files = st.file_uploader(
            "Choose files",
            type=['xlsx', 'csv'],
            accept_multiple_files=True,
            help="You can select multiple files at once"
        )

        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} files selected")

            # Display file list
            with st.expander("📋 View uploaded files"):
                for file in uploaded_files:
                    st.text(f"• {file.name}")

            # Scan button
            if st.button("🔍 Scan Files", type="primary", use_container_width=True):
                with st.spinner("Scanning files..."):
                    scan_results = []

                    # Save files temporarily and scan them
                    for uploaded_file in uploaded_files:
                        # Save to a temporary location
                        temp_path = Path("temp") / uploaded_file.name
                        temp_path.parent.mkdir(exist_ok=True)

                        with open(temp_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())

                        # Scan the file
                        result = st.session_state.processor.scan_file(temp_path)
                        scan_results.append(result)

                    st.session_state.scan_results = scan_results
                    st.session_state.files_uploaded = True
                    st.rerun()

        # Display scan results if available
        if st.session_state.scan_results:
            st.markdown("---")
            st.subheader("📊 File Validation Results")

            for result in st.session_state.scan_results:
                if result['status'] == 'ERROR':
                    st.error(f"❌ **{result['file_name']}** - Error: {result.get('error_message', 'Unknown error')}")
                elif result['status'] == 'WARNING':
                    st.warning(f"⚠️ **{result['file_name']}** - Warning: Check the details below")
                else:
                    st.success(f"✅ **{result['file_name']}** - OK")

                # Show details in an expander
                with st.expander(f"Details: {result['file_name']}"):
                    col1, col2 = st.columns(2)

                    with col1:
                        st.metric("Row Count", result.get('row_count', 'N/A'))
                        st.metric("Numeric Values", f"{result.get('value_numeric_pct', 0):.1f}%")

                    with col2:
                        st.text(f"Columns: {', '.join(result.get('columns_found', []))}")
                        st.text(f"Date Sample: {result.get('date_sample', 'N/A')}")

            # Proceed button
            st.markdown("---")
            if st.button("✅ Looks Good - Proceed to Processing", type="primary", use_container_width=True):
                st.success("Ready to process! Go to the 'Process Data' tab.")

    # ========== TAB 2: PROCESS DATA ==========
    with tab2:
        st.header("Process Data")

        if not st.session_state.files_uploaded:
            st.warning("⚠️ Please upload and scan files first (Tab 1)")
        else:
            st.info("🚀 Ready to process your data!")

            # Show configuration
            st.markdown("### Current Settings")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Time Tolerance", f"±{tolerance_minutes} minutes")
            with col2:
                st.metric("Stale Data Threshold", f"{consecutive_repeats} repeats")

            st.markdown("---")

            # Process button
            if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                with st.spinner("Processing... This may take a few moments..."):

                    # Create a progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    # Step 1: Load files
                    status_text.text("Loading files...")
                    progress_bar.progress(20)

                    file_paths = [Path("temp") / result['file_name'] for result in st.session_state.scan_results]
                    st.session_state.processor.load_multiple_files(file_paths)

                    # Step 2: Combine files
                    status_text.text("Combining sensor data...")
                    progress_bar.progress(40)
                    st.session_state.processor.combine_files()

                    # Step 3: Save minute-level data (future: SQL)
                    status_text.text("Saving minute-level data...")
                    progress_bar.progress(50)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    minute_data_path = Path("output") / f"minute_data_{timestamp}.csv"
                    st.session_state.processor.save_minute_data_csv(minute_data_path)

                    # Step 4: Resample to 15-minute intervals
                    status_text.text("Resampling to 15-minute intervals...")
                    progress_bar.progress(70)
                    st.session_state.processor.resample_to_15min(tolerance_minutes=tolerance_minutes)

                    # Step 5: Flag stale data
                    status_text.text("Flagging stale data...")
                    progress_bar.progress(85)
                    st.session_state.processor.flag_stale_data(consecutive_repeats=consecutive_repeats)

                    # Step 6: Complete
                    status_text.text("Processing complete!")
                    progress_bar.progress(100)

                    st.session_state.processing_complete = True

                st.success("✅ Processing complete! Go to the 'Results & Export' tab.")
                st.balloons()

            # Show processing log if available
            if st.session_state.processor.processing_log:
                with st.expander("📝 View Processing Log"):
                    for log_entry in st.session_state.processor.processing_log:
                        st.text(log_entry)

    # ========== TAB 3: RESULTS & EXPORT ==========
    with tab3:
        st.header("Results & Export")

        if not st.session_state.processing_complete:
            st.warning("⚠️ Please process your data first (Tab 2)")
        else:
            # Get processing summary
            summary = st.session_state.processor.get_processing_summary()

            # Display summary
            st.markdown("### 📈 Processing Summary")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Rows", summary['total_rows'])
            with col2:
                st.metric("Number of Sensors", summary['num_sensors'])
            with col3:
                st.metric("Inexact Matches", summary['inexact_matches'])
            with col4:
                st.metric("Stale Data Points", summary['stale_data_points'])

            st.markdown(f"**Date Range:** {summary['date_range']}")

            # Show sensor list
            with st.expander("📋 Sensors Included"):
                for sensor in summary['sensor_names']:
                    st.text(f"• {sensor}")

            st.markdown("---")

            # Preview the data
            st.markdown("### 👁️ Data Preview")
            if st.session_state.processor.resampled_df is not None:
                st.dataframe(
                    st.session_state.processor.resampled_df.head(20),
                    use_container_width=True
                )

            st.markdown("---")

            # Export section
            st.markdown("### 💾 Export Data")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("#### Clean 15-Minute Data")
                output_filename = st.text_input(
                    "Filename",
                    value=f"clean_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    key="clean_filename"
                )

                if st.button("📥 Download Clean CSV", type="primary", use_container_width=True):
                    output_path = Path("output") / output_filename
                    success = st.session_state.processor.export_to_csv(output_path)

                    if success:
                        # Provide download button
                        with open(output_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Click to Download",
                                data=f,
                                file_name=output_filename,
                                mime='text/csv',
                                use_container_width=True
                            )
                        st.success(f"✅ File ready: {output_filename}")

            with col2:
                st.markdown("#### Minute-Level Data")
                st.info("This has been saved to the 'output' folder. In the future, this will go to a SQL data lake.")

                # Find the most recent minute data file
                minute_files = list(Path("output").glob("minute_data_*.csv"))
                if minute_files:
                    latest_minute_file = max(minute_files, key=lambda p: p.stat().st_mtime)
                    st.text(f"📁 {latest_minute_file.name}")

                    with open(latest_minute_file, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download Minute Data",
                            data=f,
                            file_name=latest_minute_file.name,
                            mime='text/csv',
                            use_container_width=True
                        )

            # Processing log
            st.markdown("---")
            with st.expander("📝 Full Processing Log"):
                for log_entry in summary['processing_log']:
                    st.text(log_entry)

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Fischer Energy Partners Data Processing Tool | Built with Streamlit"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
