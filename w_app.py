import streamlit as st
import os
import sys
import shutil
import importlib
import tempfile

# To make sure we can import the scripts
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(SCRIPT_DIR)

# Dynamically import the main functions from the scripts
# This assumes WBW.py and WBW2.py are in the same directory as this app.
wbw_module = importlib.import_module("WBW_new")
wbw2_module = importlib.import_module("WBW2_new")

def main():
    st.set_page_config(page_title="Wallet Transaction Analysis", layout="wide")
    st.title("Wallet-by-Wallet Transaction Analysis")

    st.markdown("""
        This application uses Python scripts to perform a
        comprehensive analysis of cryptocurrency wallet data.
        * **WBW.py:** Analyzes closing position and balance data to generate detailed reports.
        * **WBW2.py:** Compares closing position and CoinTracking data to create a comparison workbook.
    """)

    st.sidebar.header("Run WBW.py Analysis")
    closing_file_wbw = st.sidebar.file_uploader("Upload 'Closing Position Report.csv'", type=['csv'], key="wbw_closing")
    balance_file_wbw = st.sidebar.file_uploader("Upload 'Balance by Exchange Report.csv'", type=['csv'], key="wbw_balance")

    if st.sidebar.button("Run WBW.py Analysis"):
        if closing_file_wbw and balance_file_wbw:
            st.info("Running WBW.py analysis...")
            # Create a temporary directory for file processing
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    # Save uploaded files to the temporary directory
                    closing_path = os.path.join(temp_dir, "Closing Position Report.csv")
                    balance_path = os.path.join(temp_dir, "Balance by Exchange Report.csv")
                    
                    with open(closing_path, "wb") as f:
                        f.write(closing_file_wbw.getvalue())
                    with open(balance_path, "wb") as f:
                        f.write(balance_file_wbw.getvalue())

                    # Call the main function with file paths as arguments
                    combined_report_path, adjusted_closing_path = wbw_module.main(closing_path, balance_path, temp_dir)

                    if combined_report_path:
                        st.success("WBW.py analysis completed successfully! Reports are available for download.")
                        
                        # Provide download buttons for the generated reports
                        st.download_button(
                            label="Download Combined Report.xlsx",
                            data=open(combined_report_path, "rb").read(),
                            file_name="Combined Report.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.error("Analysis failed. Please check the log for details.")
                        
                except Exception as e:
                    st.error(f"An error occurred during the WBW.py analysis: {e}")
        else:
            st.warning("Please upload both CSV files to run WBW.py analysis.")

    st.sidebar.header("Run WBW2.py Comparison")
    closing_file_wbw2 = st.sidebar.file_uploader("Upload 'Updated Closing Position Report.csv'", type=['csv'], key="wbw2_closing")
    ct_file_wbw2 = st.sidebar.file_uploader("Upload 'CoinTracking Import File.csv'", type=['csv'], key="wbw2_ct")

    if st.sidebar.button("Run WBW2.py Comparison"):
        if closing_file_wbw2 and ct_file_wbw2:
            st.info("Running WBW2.py comparison...")
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    # Save uploaded files to the temporary directory
                    closing_path = os.path.join(temp_dir, "Updated Closing Position Report.csv")
                    ct_path = os.path.join(temp_dir, "CoinTracking Import File.csv")

                    with open(closing_path, "wb") as f:
                        f.write(closing_file_wbw2.getvalue())
                    with open(ct_path, "wb") as f:
                        f.write(ct_file_wbw2.getvalue())
                    
                    # Call the main function with file paths as arguments
                    comparison_report_path = wbw2_module.main(closing_path, ct_path, temp_dir)

                    if comparison_report_path:
                        st.success("WBW2.py comparison completed successfully! Workbook is available for download.")
                        st.download_button(
                            label="Download Comparison Workbook",
                            data=open(comparison_report_path, "rb").read(),
                            file_name="New Closing Position vs CoinTracking Import.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.error("Comparison failed. Please check the log for details.")

                except Exception as e:
                    st.error(f"An error occurred during the WBW2.py comparison: {e}")
        else:
            st.warning("Please upload both CSV files to run WBW2.py comparison.")

if __name__ == "__main__":
    main()