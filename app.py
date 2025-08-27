# app.py
import streamlit as st
import pandas as pd
from processing_logic import process_file, CONFIGS # We'll create CONFIGS in the next step
from balance import calculate_balances
from rollforward_tool import generate_rollforward_summary

# --- 1. Page Configuration ---
st.set_page_config(
    page_title="Crypto CSV Formatter",
    page_icon="✨",
    layout="centered"
)

st.title("📄 Crypto Transaction Formatter")
st.write(
    "Upload your transaction CSV file, select the original format, and get a standardized output CSV."
)


# --- 2. File Uploader and Configuration Selector ---
uploaded_file = st.file_uploader("Upload your CSV file", type="csv")

# Create a dropdown menu from the names of your configurations
config_options = list(CONFIGS.keys())
selected_config_name = st.selectbox(
    "What is the format of the original file?",
    options=config_options
)


# --- 3. Processing Logic and Download Button ---
if st.button("Process File"):
    if uploaded_file is not None:
        with st.spinner("Processing your file... this may take a moment."):
            try:
                # Read the uploaded CSV into a pandas DataFrame
                input_df = pd.read_csv(uploaded_file)
                
                # Get the selected configuration dictionary
                selected_config = CONFIGS[selected_config_name]
                
                # Call your existing processing function
                output_df = process_file(input_df, selected_config)
                
                st.success("✅ File processed successfully!")
                
                # Display a preview of the formatted data
                balance_df = calculate_balances(output_df)

                # Use columns to display results side-by-side
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("Portfolio Balances")
                    st.dataframe(balance_df)

                with col2:
                    st.subheader("Formatted Transactions (Preview)")
                    st.dataframe(output_df.head(10)) # Show a slightly larger preview
                st.write("This is a preview of the formatted transactions. You can download the full file below.")
                
                # Prepare the file for download
                csv_data = output_df.to_csv(index=False).encode('utf-8')
                
                # Create the download button
                st.download_button(
                    label="⬇️ Download Formatted CSV",
                    data=csv_data,
                    file_name=f"formatted_{uploaded_file.name}",
                    mime='text/csv',
                )

            except Exception as e:
                st.error(f"An error occurred: {e}")
    else:
        st.warning("Please upload a CSV file first.")

# # --- Add a new section for the Rollforward Tool ---
# st.header("Function 2: Generate Crypto Rollforward Schedule")

# st.write(
#     "Upload the required crypto reports and the schedule template. "
#     "The tool will automatically calculate the totals and fill out the schedule for you."
# )

# # Create a dictionary to hold the uploaded files
# required_files = {}

# # Use columns for a cleaner layout
# col1, col2 = st.columns(2)

# with col1:
#     required_files['income_report'] = st.file_uploader("Upload Income Report CSV", type="csv")
#     required_files['capital_gain_report'] = st.file_uploader("Upload Capital Gain Report CSV", type="csv")
#     required_files['fee_report'] = st.file_uploader("Upload Fee Report CSV", type="csv")

# with col2:
#     required_files['prior_year_closing_report'] = st.file_uploader("Upload Prior Year Closing Report CSV", type="csv")
#     required_files['current_year_closing_report'] = st.file_uploader("Upload Current Year Closing Report CSV", type="csv")
#     required_files['template'] = st.file_uploader("Upload Rollforward Schedule Template CSV", type="csv")


# if st.button("Generate Rollforward Schedule"):
#     # Check if all files have been uploaded
#     if all(required_files.values()):
#         with st.spinner("Generating schedule..."):
#             try:
#                 # Call the function from your new tool file
#                 final_schedule_df = generate_rollforward_summary(required_files)

#                 st.success("✅ Rollforward schedule generated successfully!")
#                 st.dataframe(final_schedule_df)

#                 # Provide a download button for the completed schedule
#                 csv_data = final_schedule_df.to_csv(index=False, header=False).encode('utf-8')
#                 st.download_button(
#                     label="⬇️ Download Completed Schedule",
#                     data=csv_data,
#                     file_name="Completed_Rollforward_Schedule.csv",
#                     mime='text/csv',
#                 )

#             except Exception as e:
#                 st.error(f"An error occurred: {e}")
#     else:
#         st.warning("Please upload all required files before generating the schedule.")