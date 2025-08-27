import traceback
import streamlit as st
import os
import sys
import shutil
import importlib
import tempfile
import io
from contextlib import redirect_stdout, redirect_stderr

# To make sure we can import the scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

# Dynamically import the main functions from the scripts
wbw_module = importlib.import_module("WBW")
wbw2_module = importlib.import_module("WBW2")

def main():
    st.set_page_config(page_title="Wallet Transaction Analysis", layout="wide")
    st.title("Wallet-by-Wallet Transaction Analysis")

    st.markdown("""
        This application uses the `WBW.py` and `WBW2.py` scripts to perform a
        comprehensive analysis of your cryptocurrency wallet data.
        * **WBW.py:** Analyzes closing position and balance data to generate detailed reports.
        * **WBW2.py:** Compares closing position and CoinTracking data to create a comparison workbook.
    """)
    
    st.sidebar.header("Run WBW.py Analysis")
    closing_file_wbw = st.sidebar.file_uploader("Upload 'Closing Position Report.csv'", type=['csv'], key="wbw_closing")
    balance_file_wbw = st.sidebar.file_uploader("Upload 'Balance by Exchange Report.csv'", type=['csv'], key="wbw_balance")

    if st.sidebar.button("Run WBW.py Analysis"):
        if closing_file_wbw and balance_file_wbw:
            st.info("Running WBW.py analysis...")
            
            # Placeholder for log output
            log_placeholder = st.empty()
            log_stream = io.StringIO()

            with redirect_stdout(log_stream):
                with redirect_stderr(log_stream):
                    try:
                        # Read files into in-memory objects
                        closing_data = io.BytesIO(closing_file_wbw.getvalue())
                        balance_data = io.BytesIO(balance_file_wbw.getvalue())
                        
                        with tempfile.TemporaryDirectory() as temp_dir:
                            # Pass in-memory objects and temporary output path to the script
                            combined_report_path, adjusted_closing_path, error_traceback = wbw_module.main(closing_data, balance_data, temp_dir)

                            if combined_report_path:
                                st.success("WBW.py analysis completed successfully! Reports are available for download.")
                                
                                st.download_button(
                                    label="Download Combined Report.xlsx",
                                    data=open(combined_report_path, "rb").read(),
                                    file_name="Combined Report.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                                # Add download button for other files if needed
                                # For example, adjusted_closing_path
                                # if adjusted_closing_path:
                                #     st.download_button(
                                #         label="Download Updated Closing Position Report.csv",
                                #         data=open(adjusted_closing_path, "rb").read(),
                                #         file_name="Updated Closing Position Report.csv",
                                #         mime="text/csv"
                                #     )
                            else:
                                st.error("Analysis failed.")
                                if error_traceback:
                                    st.text_area("Error Details", error_traceback, height=300)

                    except Exception as e:
                        st.error(f"An unexpected error occurred: {e}")
                        st.text_area("Error Details", traceback.format_exc(), height=300)
            
            # Display captured log output
            log_output = log_stream.getvalue()
            if log_output:
                log_placeholder.text_area("Script Log", log_output, height=400)

        else:
            st.warning("Please upload both CSV files to run WBW.py analysis.")

    st.sidebar.header("Run WBW2.py Comparison")
    closing_file_wbw2 = st.sidebar.file_uploader("Upload 'Updated Closing Position Report.csv'", type=['csv'], key="wbw2_closing")
    ct_file_wbw2 = st.sidebar.file_uploader("Upload 'CoinTracking Import File.csv'", type=['csv'], key="wbw2_ct")

    if st.sidebar.button("Run WBW2.py Comparison"):
        if closing_file_wbw2 and ct_file_wbw2:
            st.info("Running WBW2.py comparison...")
            
            # Placeholder for log output
            log_placeholder = st.empty()
            log_stream = io.StringIO()

            with redirect_stdout(log_stream):
                with redirect_stderr(log_stream):
                    try:
                        # Read files into in-memory objects
                        closing_data_wbw2 = io.BytesIO(closing_file_wbw2.getvalue())
                        ct_data_wbw2 = io.BytesIO(ct_file_wbw2.getvalue())

                        with tempfile.TemporaryDirectory() as temp_dir:
                            # Pass in-memory objects and temporary output path to the script
                            comparison_report_path, error_traceback = wbw2_module.main(closing_data_wbw2, ct_data_wbw2, temp_dir)

                            if comparison_report_path:
                                st.success("WBW2.py comparison completed successfully! Workbook is available for download.")
                                st.download_button(
                                    label="Download Comparison Workbook",
                                    data=open(comparison_report_path, "rb").read(),
                                    file_name="New Closing Position vs CoinTracking Import.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                            else:
                                st.error("Comparison failed.")
                                if error_traceback:
                                    st.text_area("Error Details", error_traceback, height=300)

                    except Exception as e:
                        st.error(f"An unexpected error occurred: {e}")
                        st.text_area("Error Details", traceback.format_exc(), height=300)
            
            # Display captured log output
            log_output = log_stream.getvalue()
            if log_output:
                log_placeholder.text_area("Script Log", log_output, height=400)

        else:
            st.warning("Please upload both CSV files to run WBW2.py comparison.")

if __name__ == "__main__":
    main()