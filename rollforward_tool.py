# rollforward_tool.py
import pandas as pd
import io

def generate_rollforward_summary(files: dict) -> pd.DataFrame:
    """
    Generates a crypto roll-forward summary by reading multiple report CSVs,
    aggregating the data, and populating a template.

    Args:
        files (dict): A dictionary where keys are report names (e.g., 'income_report')
                      and values are the uploaded file objects from Streamlit.

    Returns:
        pd.DataFrame: The populated roll-forward schedule as a DataFrame.
    """
    # --- 1. Read and Process Source Reports ---

    # Read the reports, using a helper function to clean them
    # def read_and_clean_csv(file_key, skip=0):
    #     df = pd.read_csv(files[file_key], skiprows=skip)
    #     # THIS IS THE FIX: Strip whitespace from all column names
    #     df.columns = df.columns.str.strip()
    #     return df
    def read_and_clean_csv(file_key, key_column, skip=0):
        """
        Reads a CSV, cleans column names, and removes summary/footer rows.
        """
        df = pd.read_csv(files[file_key], skiprows=skip)
        
        # Step 1: Strip whitespace from all column names (fixes KeyErrors)
        df.columns = df.columns.str.strip()
        
        # Step 2 (NEW): Remove summary rows at the bottom of the file.
        # We do this by checking if the key_column contains valid data.
        
        # First, convert the key column, forcing any non-matching values (like empty cells) into 'NaT' or 'NaN'
        if 'date' in str(key_column).lower():
            df[key_column] = pd.to_datetime(df[key_column], errors='coerce')
        else:
            df[key_column] = pd.to_numeric(df[key_column], errors='coerce')
        
        # Now, drop any row where the key column is 'NaT' or 'NaN'
        df.dropna(subset=[key_column], inplace=True)
        
        return df

    # Call the cleaning function for each report, providing the correct key column
    df_income = read_and_clean_csv('income_report', key_column='Date of deposit')
    df_capital_gain = read_and_clean_csv('capital_gain_report', key_column='Date Sold')
    df_fees = read_and_clean_csv('fee_report', key_column='Fee date', skip=1) # Skip title row
    df_prior_closing = read_and_clean_csv('prior_year_closing_report', key_column='Amount')
    df_current_closing = read_and_clean_csv('current_year_closing_report', key_column='Amount')
    
    # Read the template file
    template_df = pd.read_csv(files['template'], header=None)
    # Rename columns for easier access
    template_df.columns = ['Field', 'Value']


    # --- 2. Perform Aggregations ---

    # Ensure numeric columns are treated as numbers, filling errors with 0
    df_capital_gain['Gain/Loss in USD'] = pd.to_numeric(df_capital_gain['Gain/Loss in USD'], errors='coerce').fillna(0)
    df_income['Value upon deposit in USD'] = pd.to_numeric(df_income['Value upon deposit in USD'], errors='coerce').fillna(0)
    df_fees['Cost Basis in USD'] = pd.to_numeric(df_fees['Cost Basis in USD'], errors='coerce').fillna(0)

    # Calculate capital gains and losses
    net_capital_gain = df_capital_gain[df_capital_gain['Gain/Loss in USD'] > 0]['Gain/Loss in USD'].replace('USD', '').sum()
    net_capital_loss = df_capital_gain[df_capital_gain['Gain/Loss in USD'] < 0]['Gain/Loss in USD'].replace('USD', '').sum()

    # Summarize income by type
    income_summary = df_income.groupby('Type')['Value upon deposit in USD'].sum()

    # Summarize fees by type
    # Clean up the 'Type' column to get consistent fee names
    df_fees['Fee Type'] = df_fees['Type'].str.replace('Paid ', '').str.replace(' fee of', '').str.strip().str.title()
    fee_summary = df_fees.groupby('Fee Type')['Cost Basis in USD'].sum()
    #rint(fee_summary)
    
    # Calculate closing cost basis
    prior_year_basis = (df_prior_closing['Cost Basis in USD'].replace('$', '', regex=True)
                        .replace(',', '', regex=True)
                        .fillna(0).astype(float).sum())
    print(prior_year_basis)
    # need to cut after "total"
    current_year_basis = (df_current_closing['Cost Basis in USD'].replace('$', '', regex=True)
                        .replace(',', '', regex=True)
                        .fillna(0).astype(float).sum())
    df2 = df_current_closing.set_index('Amount', drop=False)
    print(df2.loc['Total':, 'Cost Basis in USD'])
    indexlist = df_current_closing.columns[(df_current_closing.values=='Total').any(0)].tolist()
    print(indexlist)
    print(df_current_closing.head())
    print(df_current_closing.at['Total', 'Cost Basis in USD'])
    print(current_year_basis)

    # --- 3. Populate the Template DataFrame ---

    # Helper function to safely update the template
    def update_value(field_name, value):
        template_df.loc[template_df['Field'] == field_name, 'Value'] = value

    # Populate all fields from our calculations
    update_value('Total cost basis per prior year Closing Position Report:', prior_year_basis)
    update_value('Net Captial Gain', net_capital_gain)
    update_value('Net Capital Loss', net_capital_loss)
    
    # Populate income types
    update_value('Airdrop', income_summary.get('Airdrop', 0))
    update_value('Income', income_summary.get('Income', 0))
    # ... you can add more income types here if they exist

    # Populate fee types
    update_value('Trade Fee', fee_summary.get('Trade', 0))
    update_value('Withdrawal Fee', fee_summary.get('Withdrawal', 0))
    # ... you can add more fee types here

    update_value('Total cost basis per current year closing position report:', current_year_basis)

    # --- 4. Final Calculations ---
    additions = template_df.loc[3:15, 'Value'].astype(float).sum()
    subtractions = template_df.loc[18:30, 'Value'].astype(float).sum()
    
    calculated_ending_basis = prior_year_basis + additions + subtractions
    variance = calculated_ending_basis - current_year_basis

    update_value('Calculated Ending Cost Basis:', calculated_ending_basis)
    update_value('Variance:', variance)

    return template_df