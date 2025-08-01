# processing_logic.py
# Imports
import pandas as pd
import numpy as np 
from datetime import datetime
#import matplotlib.pyplot as plt
#import io

# --- CONFIGURATIONS ---
coinbase_pro_config = {
    "platform_name": "Coinbase Pro",
    "consolidation_style": "by_trade_id_and_time", # Use the leg-based consolidation
    "identification_headers": ["portfolio", "type", "time", "amount", "balance"],
    "column_mapping": {
        "time": "DateTime_Raw",
        "type": "Transaction_Type_Raw",
        "amount": "Amount_Raw",
        "amount/balance unit": "Currency_Raw",
        "trade id": "Trade_ID_Raw",
        "order id": "Order_ID_Raw",
        "transfer id": "Transfer_ID_Raw",
        "portfolio": "Portfolio_Raw"
    },
    "static_values": {
        "Exchange": "Coinbase Pro",
        "Imported From": "Coinbase Pro CSV",
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
    "transformations": {
        "Date": {"source": "DateTime_Raw", "action": "extract_datetime_combined"},
        "Type": {"source": "Transaction_Type_Raw", "action": "map_transaction_type"},
        "Buy": {"source": ["Transaction_Type_Raw", "Amount_Raw"], "action": "get_buy_amount_from_leg"},
        "Sell": {"source": ["Transaction_Type_Raw", "Amount_Raw"], "action": "get_sell_amount_from_leg"},
        "Fee": {"source": ["Transaction_Type_Raw", "Amount_Raw"], "action": "get_fee_amount_from_leg"},
        "Trade ID": {"source": "Trade_ID_Raw", "action": "passthrough"},
        "Group": {"source": "Portfolio_Raw", "action": "passthrough"}
    }
}

# --- NEW: Bitcoin.tax Config (Handles pre-consolidated rows) ---
bitcoin_tax_config = {
    "platform_name": "Bitcoin.tax",
    "consolidation_style": "direct", # Use the new direct processing function
    "identification_headers": ["Date", "Action", "Symbol", "Volume", "Cost/Proceeds"],
    "column_mapping": {
        "Date": "DateTime_Raw",
        "Action": "Operation_Raw",
        "Symbol": "Currency_Raw",
        "Volume": "Buy_Amount_Raw",
        "Currency": "Pair_Currency_Raw",
        "Cost/Proceeds": "Sell_Amount_Raw",
        "Fee": "Fee_Raw",
        "FeeCurrency": "Fee_Currency_Raw",
        "Account": "Exchange_Raw",
        "Subaccount": "Group_Raw",
        "ExchangeId": "Trade_ID_Raw",
        "Memo": "Comment_Raw"
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

# --- NEW: Configuration for Binance US (Direct Style) ---
binance_us_config = {
    "platform_name": "Binance US",
    "consolidation_style": "direct", # Use the direct processing function
    "identification_headers": ["Time", "Category", "Operation", "Base Asset", "Quote Asset"],
    "column_mapping": {
        "User ID": "Group_Raw",
        "Time": "DateTime_Raw",
        "Category": "Category_Raw",
        "Operation": "Operation_Raw",
        "Order ID": "Order_ID_Raw",
        "Transaction ID": "Trade_ID_Raw",
        # Columns for Trades/Swaps
        "Base Asset": "Currency_Raw",
        "Realized Amount For Base Asset": "Buy_Amount_Raw",
        "Quote Asset": "Pair_Currency_Raw",
        "Realized Amount for Quote Asset": "Sell_Amount_Raw",
        "Fee Asset": "Fee_Currency_Raw",
        "Realized Amount for Fee Asset": "Fee_Raw",
        # Columns for Deposits/Withdrawals
        "Primary Asset": "Primary_Asset_Raw",
        "Realized Amount For Primary Asset": "Primary_Amount_Raw",
        "Additional Note": "Comment_Raw"
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

# --- NEW: Configuration for MEXC ---
mexc_config = {
    "platform_name": "MEXC",
    "consolidation_style": "pair", # Separate pairs before using the direct processing function
    "identification_headers": ["Pairs", "Time", "Side", "Executed Amount", "Total"],
    "column_mapping": {
        "Time": "DateTime_Raw",
        "Side": "Operation_Raw",
        "Pairs": "Pair_Raw",
        "Executed Amount": "Buy_Amount_Raw",
        "Total": "Sell_Amount_Raw",
        "Fee": "Fee_Raw",
        "Role": "Comment_Raw"
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

# --- NEW: Configuration for KuCoin ---
kucoin_config = {
    "platform_name": "KuCoin",
    "consolidation_style": "pair", # Separate pairs before direct processing function
    "identification_headers": ["UID", "Symbol", "Side", "Filled Amount", "Filled Volume"],
    "column_mapping": {
        "UID": "Group_Raw",
        "Order ID": "Order_ID_Raw",
        "Symbol": "Pair_Raw",
        "Side": "Operation_Raw",
        "Filled Amount": "Buy_Amount_Raw",
        "Filled Volume": "Sell_Amount_Raw",
        "Filled Time(UTC+00:00)": "DateTime_Raw",
        "Fee": "Fee_Raw",
        "Fee Currency": "Fee_Currency_Raw",
        "Maker/Taker": "Comment_Raw"
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

koinly_config = {
    "platform_name": "Koinly",
    "consolidation_style": "direct", 
    "identification_headers": ["Date (UTC)", "From Wallet (read-only)", "From Currency", "To Amount", "Net Value"],
    "column_mapping": {
        "ID (read-only)": "Trade_ID_Raw",
        "Date (UTC)": "DateTime_Raw",
        "Type": "Category_Raw",
        "Tag": "Operation_Raw",
        "From Wallet (read-only)": "Group_Raw",
        "To Amount": "Buy_Amount_Raw",
        "To Currency": "Currency_Raw",
        "From Amount": "Sell_Amount_Raw",
        "From Currency": "Pair_Currency_Raw",
        "Fee Amount": "Fee_Raw",
        "Fee Currency": "Fee_Currency_Raw",
        "Description": "Comment_Raw"
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

awaken_tax_config = {
    "platform_name": "Awaken Tax",
    "consolidation_style": "multi", # Multi blockchain consolidation, with multi variables in single column
    "identification_headers": ["Priority", "Provider", "Title", "Hash", "Sent", "Received"],
    "column_mapping": {
        "ID": "Trade_ID_Raw",
        "Priority": "Priority_Raw",
        "Provider": "Exchange_Raw",
        "Title": "Operation_Raw",
        "Date": "DateTime_Raw",
        "Notes": "Comment_Raw",
        "Hash": "Hash_Raw",
        "Cap Gains (USD)": "Cap_Gains_Raw",
        "Sent": "Sent_Raw",
        "Received": "Received_Raw",
        "Fees": "Fee_Raw",
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

stake_tax_config = {
    "platform_name": "Stake Tax",
    "consolidation_style": "direct",
    "identification_headers": ["timestamp", "tx_type", "received_amount", "received_currency", "sent_amount", "sent_currency", "fee", "fee_currency"],
    "column_mapping": {
        "timestamp": "DateTime_Raw",
        "tx_type": "Category_Raw",
        "received_amount": "Buy_Amount_Raw",
        "received_currency": "Currency_Raw",
        "sent_amount": "Sell_Amount_Raw",
        "sent_currency": "Pair_Currency_Raw",
        "fee": "Fee_Raw",
        "fee_currency": "Fee_Currency_Raw",
        "comment": "OG_Comment_Raw",
        "url": "Comment_Raw",
        "exchange": "Exchange_Raw",
        "wallet_address": "Group_Raw",
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

polygon_zkevm_config = {
    "platform_name": "Polygon ZKevm",
    "consolidation_style": "direct",
    "identification_headers": ['DateTime', 'From', 'From_Nametag', 'To', 'To_Nametag', 'Amount'],
    "column_mapping": {
        "Transaction Hash": "Trade_ID_Raw",
        "Parent Transaction Hash": "Parent_Transaction_ID_Raw",
        "Status": "Status_Raw",
        "Method": "Category_Raw",
        "DateTime": "DateTime_Raw",
        "From": "From_Raw",
        "To": "To_Raw",
        "Amount": "Amount_Cur_Raw",
        "Txn Fee": "Fee_Raw",
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}

nexo_config = {
    "platform_name": "Nexo",
    "consolidation_style": "direct",
    "identification_headers": ["Transaction", "Type", "Input Currency", "Input Amount", "Output Currency", "Output Amount", "Fee", "Fee Currency", "Date / Time (UTC)"],
    "column_mapping": {
        "Transaction": "Trade_ID_Raw",
        "Type": "Category_Raw",
        "Input Currency": "Currency_Raw",
        "Input Amount": "Buy_Amount_Raw",
        "Output Currency": "Pair_Currency_Raw",
        "Output Amount": "Sell_Amount_Raw",
        "Fee": "Fee_Raw",
        "Fee Currency": "Fee_Currency_Raw",
        "Details": "Comment_Raw",
        "Date / Time (UTC)": "DateTime_Raw",
    },
    "target_columns": ['Type', 'Buy', 'Cur.', 'Sell', 'Cur..1', 'Fee', 'Cur..2', 'Exchange', 'Group', 'Comment', 'Date'],
}
# Add any other configs you have

# Create a dictionary to hold all configs for easy access in the app
CONFIGS = {
    "Coinbase Pro": coinbase_pro_config,
    "Bitcoin.tax": bitcoin_tax_config,
    "Binance US": binance_us_config,
    "MEXC": mexc_config,
    "KuCoin": kucoin_config,
    "Koinly": koinly_config,
    "Awaken Tax": awaken_tax_config,
    "Stake Tax": stake_tax_config,
    "Polygon ZKevm": polygon_zkevm_config,
    "Nexo": nexo_config
    # Add other mappings here
}


# --- HELPER & PROCESSING FUNCTIONS ---
# ... (all your functions like extract_datetime_combined, process_file, etc.)
# --- 2. Helper Functions for Transformations ---
# New function to extract combined datetime string
def extract_datetime_combined(dt_str):
    if isinstance(dt_str, (pd.Series, pd.Index)):
        if not dt_str.empty:
            dt_str = dt_str.iloc[0]
        else:
            return ''
    if pd.isna(dt_str) or dt_str == '':
        return ''
    try:
        # Use errors='coerce' to return NaT for unparseable dates
        dt_obj = pd.to_datetime(dt_str, errors='coerce')
        if pd.isna(dt_obj):
            return ''
        return dt_obj.strftime('%d-%m-%Y %H:%M:%S')
    except Exception as e:
        # This block should ideally not be hit with errors='coerce', but good for extreme cases
        print(f"Error formatting datetime '{dt_str}': {e}")
        return ''    

def map_transaction_type(raw_type):
    if raw_type == 'deposit':
        return 'Deposit'
    elif raw_type == 'withdrawal':
        return 'Withdrawal'
    elif raw_type == 'match':
        return 'Trade_Leg'
    elif raw_type == 'fee':
        return 'Fee_Leg'
    elif raw_type == 'conversion':
        return 'Swap_Leg'
    return 'Other'

def get_buy_amount_from_leg(raw_type, amount_raw):
    amount = float(amount_raw)
    if raw_type == 'deposit' or ((raw_type == 'match' or raw_type == 'conversion') and amount > 0):
        return abs(amount)
    return 0.0

def get_sell_amount_from_leg(raw_type, amount_raw):
    amount = float(amount_raw)
    if raw_type == 'withdrawal' or ((raw_type == 'match' or raw_type == 'conversion') and amount < 0):
        return abs(amount)
    return 0.0

def get_fee_amount_from_leg(raw_type, amount_raw):
    amount = float(amount_raw)
    if raw_type == 'fee':
        return abs(amount)
    return 0.0

def passthrough(value):
    return value

# Map action names to helper functions
transformation_actions = {
    "extract_datetime_combined": extract_datetime_combined,
    "map_transaction_type": map_transaction_type,
    "get_buy_amount_from_leg": get_buy_amount_from_leg,
    "get_sell_amount_from_leg": get_sell_amount_from_leg,
    "get_fee_amount_from_leg": get_fee_amount_from_leg,
    "passthrough": passthrough,
}

# --- 4. Function for Trade Consolidation (Updated Currency Logic) ---
def consolidate_trade_rows(intermediate_df, config):
    final_rows = []

    # Separate deposits and withdrawals
    deposits_withdrawals_df = intermediate_df[
        (intermediate_df['Type_Intermediate'] == 'Deposit') |
        (intermediate_df['Type_Intermediate'] == 'Withdrawal')
    ].copy()

    for _, row in deposits_withdrawals_df.iterrows():
        new_row = {col: '' for col in config["target_columns"]}
        new_row['Type'] = row['Type_Intermediate']
        new_row['Date'] = extract_datetime_combined(row['DateTime_Raw'])
        new_row['Exchange'] = row['Exchange']
        new_row['Group'] = row['Group']
        #new_row['Imported From'] = row['Imported From']
        #new_row['Add Date'] = row['Add Date']

        if row['Type_Intermediate'] == 'Deposit':
            new_row['Buy'] = row['Buy']
            new_row['Cur.'] = row['Currency_Raw'] # Take raw currency for deposits/withdrawals
            new_row['Comment'] = f"Deposit (Transfer ID: {row['Transfer_ID_Raw']})"
        elif row['Type_Intermediate'] == 'Withdrawal':
            new_row['Sell'] = row['Sell']
            new_row['Cur..1'] = row['Currency_Raw'] # Take raw currency for deposits/withdrawals
            new_row['Comment'] = f"Withdrawal (Transfer ID: {row['Transfer_ID_Raw']})"
        final_rows.append(new_row)

    # Filter for trade and fee legs
    trade_legs_df = intermediate_df[
        (intermediate_df['Type_Intermediate'] == 'Trade_Leg') |
        (intermediate_df['Type_Intermediate'] == 'Fee_Leg')
    ].copy()

    if not trade_legs_df.empty:
        grouped_trades = trade_legs_df.groupby(['Trade_ID_Raw', 'DateTime_Raw'], dropna=False)

        for (trade_id_val, datetime_val), group in grouped_trades:
            consolidated_row = {col: '' for col in config["target_columns"]}

            # Populate common fields
            consolidated_row['Type'] = 'Trade'
            consolidated_row['Date'] = extract_datetime_combined(datetime_val)
            #consolidated_row['Trade ID'] = trade_id_val if pd.notna(trade_id_val) else ''
            first_row = group.iloc[0]
            consolidated_row['Exchange'] = first_row['Exchange']
            consolidated_row['Group'] = first_row['Group']

            # Aggregate Buy/Sell/Fee amounts
            total_buy = group['Buy'].sum()
            total_sell = group['Sell'].sum()
            total_fee = group['Fee'].sum()

            consolidated_row['Buy'] = total_buy if total_buy > 0 else np.nan
            consolidated_row['Sell'] = total_sell if total_sell > 0 else np.nan
            consolidated_row['Fee'] = total_fee if total_fee > 0 else np.nan

            # --- UPDATED CURRENCY DETERMINATION LOGIC ---
            # Buy currency: Find the currency from the leg that contributed to total_buy
            buy_currency = ''
            if total_buy > 0:
                buy_leg = group[group['Buy'] > 0]
                if not buy_leg.empty:
                    buy_currency = buy_leg['Currency_Raw'].iloc[0]
            consolidated_row['Cur.'] = buy_currency

            # Sell currency: Find the currency from the leg that contributed to total_sell
            sell_currency = ''
            if total_sell > 0:
                sell_leg = group[group['Sell'] > 0]
                if not sell_leg.empty:
                    sell_currency = sell_leg['Currency_Raw'].iloc[0]
            consolidated_row['Cur..1'] = sell_currency

            # Fee currency: Find the currency from the leg that contributed to total_fee
            fee_currency = ''
            if total_fee > 0:
                fee_leg = group[group['Fee'] > 0]
                if not fee_leg.empty:
                    fee_currency = fee_leg['Currency_Raw'].iloc[0]
            consolidated_row['Cur..2'] = fee_currency
            # --- END UPDATED CURRENCY DETERMINATION LOGIC ---


            # Generate comment for consolidated trade
            comment_parts = []
            if total_buy > 0 and consolidated_row['Cur.']:
                comment_parts.append(f"Buy {total_buy:.8f} {consolidated_row['Cur.']}")
            if total_sell > 0 and consolidated_row['Cur..1']:
                comment_parts.append(f"Sell {total_sell:.8f} {consolidated_row['Cur..1']}")
            if total_fee > 0 and consolidated_row['Cur..2']:
                comment_parts.append(f"Fee {total_fee:.8f} {consolidated_row['Cur..2']}")

            base_comment = f"Trade (Trade ID: {trade_id_val})" if pd.notna(trade_id_val) else "Trade"
            if comment_parts:
                consolidated_row['Comment'] = f"{base_comment}: {', '.join(comment_parts)}"
            else:
                consolidated_row['Comment'] = base_comment

            final_rows.append(consolidated_row)
            
    # Process 'Swap' (conversion) legs
    swap_legs_df = intermediate_df[intermediate_df['Type_Intermediate'] == 'Swap_Leg'].copy()
    if not swap_legs_df.empty:
        # Group by Order_ID_Raw (since no Trade ID) and DateTime_Raw
        grouped_swaps = swap_legs_df.groupby(['DateTime_Raw'], dropna=False)

        for key_tuple, group in grouped_swaps:
            datetime_val = key_tuple[0]
            consolidated_row2 = {col: '' for col in config["target_columns"]}
        
            consolidated_row2['Date'] = extract_datetime_combined(datetime_val)
            # No Trade ID for swaps, leave it empty
            #consolidated_row2['Trade ID'] = ''
            first_row = group.iloc[0]
            consolidated_row2['Exchange'] = first_row['Exchange']
            consolidated_row2['Group'] = first_row['Group']

            total_buy = group['Buy'].sum()
            total_sell = group['Sell'].sum()
            # No fee for conversions, so total_fee will be 0
            total_fee = 0.0

            consolidated_row2['Buy'] = total_buy if total_buy > 0 else np.nan
            consolidated_row2['Sell'] = total_sell if total_sell > 0 else np.nan
            consolidated_row2['Fee'] = total_fee # Ensure fee is 0

            buy_currency = ''
            if total_buy > 0:
                buy_leg = group[group['Buy'] > 0]
                if not buy_leg.empty:
                    buy_currency = buy_leg['Currency_Raw'].iloc[0]
            consolidated_row2['Cur.'] = buy_currency

            sell_currency = ''
            if total_sell > 0:
                sell_leg = group[group['Sell'] > 0]
                if not sell_leg.empty:
                    sell_currency = sell_leg['Currency_Raw'].iloc[0]
            consolidated_row2['Cur..1'] = sell_currency

            # Fee currency will be empty as there's no fee
            consolidated_row2['Cur..2'] = ''

            if buy_currency.lower()==("w" + sell_currency.lower()) or sell_currency.lower()==("w" + buy_currency.lower()):
                consolidated_row2['Type'] = 'Swap (non taxable)' # Final type is 'Swap (non taxable)'
                base_comment = "Swap (non taxable)"
                print("DEBUG Swap (non taxable) found")
            else:
                consolidated_row2['Type'] = 'Trade'
                base_comment = "Trade"
                print("DEBUG trade found")

            comment_parts = []
            if total_buy > 0 and consolidated_row2['Cur.']:
                comment_parts.append(f"Buy {total_buy:.8f} {consolidated_row2['Cur.']}")
            if total_sell > 0 and consolidated_row2['Cur..1']:
                comment_parts.append(f"Sell {total_sell:.8f} {consolidated_row2['Cur..1']}")

            
            if comment_parts:
                consolidated_row2['Comment'] = f"{base_comment}: {', '.join(comment_parts)}"
            else:
                consolidated_row2['Comment'] = base_comment

            final_rows.append(consolidated_row2)

    final_df = pd.DataFrame(final_rows, columns=config["target_columns"])

    # Fill NaN values in numeric columns with 0 for cleaner output CSV
    for col in ['Buy', 'Sell', 'Fee']:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

    print("DEBUG Dates")
    print(final_df['Date'])        
            
    # Create a temporary datetime column for robust sorting
    final_df['Sort_DateTime'] = pd.to_datetime(final_df['Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
    final_df = final_df.sort_values(by='Sort_DateTime').drop(columns=['Sort_DateTime'])

    # Explicitly cast the 'Date' column to string to prevent re-formatting by to_csv
    final_df['Date'] = final_df['Date'].astype(str)

    print("DEBUG Dates")
    print(final_df['Date'])   

    return final_df

# --- 3. Processing Workflows ---
# --- WORKFLOW 1: For Leg-Based Formats (like Coinbase Pro) ---
def process_to_intermediate_legs(input_df, config):
    renamed_df = input_df.rename(columns=config["column_mapping"])
    for _, raw_col in config["column_mapping"].items():
        if raw_col not in renamed_df.columns:
            renamed_df[raw_col] = np.nan

    intermediate_df = pd.DataFrame()
    intermediate_df['Type_Intermediate'] = renamed_df.apply(
        lambda row: transformation_actions["map_transaction_type"](row.get('Transaction_Type_Raw')), axis=1
    )
    intermediate_df['DateTime_Raw'] = renamed_df['DateTime_Raw']
    intermediate_df['Amount_Raw'] = pd.to_numeric(renamed_df['Amount_Raw'], errors='coerce')
    intermediate_df['Currency_Raw'] = renamed_df['Currency_Raw']
    intermediate_df['Trade_ID_Raw'] = renamed_df['Trade_ID_Raw']
    intermediate_df['Order_ID_Raw'] = renamed_df['Order_ID_Raw']
    intermediate_df['Transfer_ID_Raw'] = renamed_df['Transfer_ID_Raw']

    for target_col, transform_def in config["transformations"].items():
        action = transform_def["action"]
        source_cols = transform_def["source"]
        if isinstance(source_cols, str):
            intermediate_df[target_col] = renamed_df.apply(
                lambda row: transformation_actions[action](row.get(source_cols)), axis=1
            )
        else:
            intermediate_df[target_col] = renamed_df.apply(
                lambda row: transformation_actions[action](*[row.get(col) for col in source_cols]), axis=1
            )

    for col, value in config["static_values"].items():
        intermediate_df[col] = value
    #intermediate_df['Add Date'] = datetime.now().strftime('%Y-%m-%d')
    return intermediate_df

def consolidate_legs_to_final_df(intermediate_df, config):
    # This is your original 'consolidate_trade_rows' function
    # It remains unchanged, as its logic is sound for its purpose.
    final_df = consolidate_trade_rows(intermediate_df, config)
    return final_df

# --- WORKFLOW 2 UPGRADED: Direct Processing Function for Pre-Consolidated Formats ---
def process_csv_direct(input_df, config):
    renamed_df = input_df.rename(columns=config["column_mapping"])
    final_rows = []
    platform = config["platform_name"]

    for _, row in renamed_df.iterrows():
        new_row = {col: '' for col in config["target_columns"]}
        add_row = {col: '' for col in config["target_columns"]}
        add_row2 = {col: '' for col in config["target_columns"]}

        # --- Populate Common Fields ---
        new_row['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
        #new_row['Trade ID'] = row.get('Trade_ID_Raw', '')
        # #new_row['Imported From'] = f"{platform} CSV"
        #new_row['Add Date'] = datetime.now().strftime('%Y-%m-%d')
        maincomment = row.get('Comment_Raw', '')
        new_row['Exchange'] = row.get('Exchange_Raw', platform) # Default to platform name, can be overridden
        group = str(row.get('Group_Raw', '')).strip() if not pd.isna(row.get('Group_Raw', '')) else ''
        if (';' in group):
            name, id = group.split(';')
            group = name.strip()
        new_row['Group'] = group

        operation = str(row.get('Operation_Raw', '')).lower()
        category = str(row.get('Category_Raw', '')).lower()
        new_row['Fee'] = pd.to_numeric(row.get('Fee_Raw'), errors='coerce')
        currency = row.get('Currency_Raw', '').strip() if not pd.isna(row.get('Currency_Raw', '')) else ''
        pair_currency = row.get('Pair_Currency_Raw', '').strip() if not pd.isna(row.get('Pair_Currency_Raw', '')) else ''
        fee_currency = row.get('Fee_Currency_Raw', '').strip() if not pd.isna(row.get('Fee_Currency_Raw', '')) else ''

        if (';' in fee_currency):
            cur, id = fee_currency.split(';')
            fee_currency = cur.strip()
        new_row['Cur..2'] = fee_currency

        if (';' in currency):
            cur, id = currency.split(';')
            currency = cur.strip()
        if (';' in pair_currency):
            cur, id = pair_currency.split(';')
            pair_currency = cur.strip()

        if config["consolidation_style"] == "pair":
            # Like MEXC, when we need to handle pairs separately
            pair = row.get('Pair_Raw', '')
            if pair:
                if '_' in pair:
                    base, quote = pair.split('_')
                elif '-' in pair:
                    base, quote = pair.split('-')
                elif '/' in pair:
                    base, quote = pair.split('/')
                elif ';' in pair:
                    base, quote = pair.split(';')
                else:
                    base, quote = pair.split(' ') if ' ' in pair else (pair, '')
                
                if operation == 'buy':
                    new_row['Cur.'] = base.strip()
                    new_row['Cur..1'] = quote.strip()
                    new_row['Cur..2'] = quote.strip() if fee_currency is None else fee_currency
                elif operation == 'sell':
                    new_row['Cur.'] = quote.strip()
                    new_row['Cur..1'] = base.strip()
                    new_row['Cur..2'] = base.strip() if fee_currency is None else fee_currency
            else:
                new_row['Cur.'] = currency
                new_row['Cur..1'] = pair_currency
        
        if category == 'transfer':
            if (currency is not None and currency != '' and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Type'] = 'Deposit'
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency
            elif (pair_currency is not None and pair_currency != '' and (row.get('Sell_Amount_Raw') is not None and row.get('Sell_Amount_Raw') != 0)):
                new_row['Type'] = 'Withdrawal'
                new_row['Sell'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                new_row['Cur..1'] = pair_currency
        elif category == 'deposit' or category == 'transfer in' or category == 'top up crypto':
            new_row['Type'] = 'Reward / Bonus' if operation == 'reward' else 'Deposit'
            #new_row['Type'] = 'Deposit'
            if (row.get('Primary_Asset_Raw') is not None and row.get('Primary_Amount_Raw') is not None):
                new_row['Buy'] = pd.to_numeric(row.get('Primary_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = row.get('Primary_Asset_Raw')
                print("DEBUG Deposit found")
            else:
                if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                    new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                    new_row['Cur.'] = currency
        elif category == 'withdrawal' or category == 'transfer out':
            new_row['Type'] = 'Withdrawal'
            if (row.get('Primary_Asset_Raw') is not None and row.get('Primary_Amount_Raw') is not None):
                new_row['Sell'] = pd.to_numeric(row.get('Primary_Amount_Raw'), errors='coerce')
                new_row['Cur..1'] = row.get('Primary_Asset_Raw')
            else:
                if (pair_currency is not None and (row.get('Sell_Amount_Raw') is not None and row.get('Sell_Amount_Raw') != 0)):
                    new_row['Sell'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                    new_row['Cur..1'] = pair_currency
        elif category == 'spend':
            new_row['Type'] = 'Spend'
            if (pair_currency is not None and (row.get('Sell_Amount_Raw') is not None and row.get('Sell_Amount_Raw') != 0)):
                new_row['Sell'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                new_row['Cur..2'] = pair_currency
        elif category == 'convert':
            if (currency.lower() == "w" + pair_currency.lower()) or (pair_currency.lower() == "w" + currency.lower()):
                new_row['Type'] = 'Swap (non taxable)'  # For conversions of wrapped crypto, we treat them as swaps
                # For Binance 'Convert', Base is what you sold, Quote is what you bought
                new_row['Sell'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur..1'] = currency
                new_row['Buy'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = pair_currency
            else:
                new_row['Type'] = 'Trade'
                new_row['Sell'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Buy'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                if not config["consolidation_style"] == "pair":
                    new_row['Cur..1'] = currency
                    new_row['Cur.'] = pair_currency
        elif category == 'airdrop':
            new_row['Type'] = 'Airdrop'
            if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency
        elif category == 'gift' or category == 'tip':
            new_row['Type'] = 'Gift / Tip'
            if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency
        elif category == 'income':
            new_row['Type'] = 'Income'
            if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency 
        elif category == '_self_transfer' or category == '_unknown':
            new_row['Type'] = 'Other Fee'      
        elif category == 'stake' or category == 'staking' or category == 'fixed term interest' or category == 'staking reward':
            new_row['Type'] = 'Staking'
            if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency
            comment = row.get('OG_Comment_Raw', '').strip()
            if ('undelegated' in comment.lower() and '[' in comment and ']' in comment):
                add_row['Type'] = 'Deposit'
                add_row['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
                maincomment = 'UNSTAKING     ' + maincomment.strip()
                add_row['Comment'] = maincomment
                add_row['Exchange'] = row.get('Exchange_Raw', platform)
                comment2 = comment.split('[')[1].split(']')[0].split(' ')
                add_row2['Type'] = 'Withdrawal'
                add_row2['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
                add_row2['Comment'] = maincomment
                exchange = row.get('Exchange_Raw', platform)
                staking = exchange.lower().replace('blockchain', 'staking') if 'blockchain' in exchange.lower() else 'staking'
                add_row2['Exchange'] = staking
                if len(comment2) > 2:
                    add_row['Buy'] = pd.to_numeric(comment2[1], errors='coerce')
                    add_row['Cur.'] = comment2[2].strip()
                    add_row2['Sell'] = pd.to_numeric(comment2[1], errors='coerce')
                    add_row2['Cur..1'] = comment2[2].strip()
                final_rows.append(add_row)
                final_rows.append(add_row2)
            elif ('delegated' in comment.lower() and '[' in comment and ']' in comment):
                add_row['Type'] = 'Withdrawal'
                add_row['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
                maincomment = 'STAKING     ' + maincomment.strip()
                add_row['Comment'] = maincomment
                add_row['Exchange'] = row.get('Exchange_Raw', platform)
                comment2 = comment.split('[')[1].split(']')[0].split(' ')
                add_row2['Type'] = 'Deposit'
                add_row2['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
                add_row2['Comment'] = maincomment
                exchange = row.get('Exchange_Raw', platform)
                staking = exchange.lower().replace('blockchain', 'staking') if 'blockchain' in exchange.lower() else 'staking'
                add_row2['Exchange'] = staking
                if len(comment2) > 2:
                    add_row['Sell'] = pd.to_numeric(comment2[1], errors='coerce')
                    add_row['Cur..1'] = comment2[2].strip()
                    add_row2['Buy'] = pd.to_numeric(comment2[1], errors='coerce')
                    add_row2['Cur.'] = comment2[2].strip()
                final_rows.append(add_row)
                final_rows.append(add_row2)
        elif category == '_msgdelegate' or category == 'locking term deposit':
            new_row['Type'] = 'Withdrawal'
            maincomment = 'STAKING     ' + maincomment.strip()
            add_row['Type'] = 'Deposit'
            add_row['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
            add_row['Comment'] = maincomment
            exchange = row.get('Exchange_Raw', platform)
            staking = exchange.lower().replace('blockchain', 'staking') if 'blockchain' in exchange.lower() else 'staking'
            add_row['Exchange'] = staking
            if (pair_currency is not None and pair_currency != '' and (row.get('Sell_Amount_Raw') is not None and row.get('Sell_Amount_Raw') != 0)):
                new_row['Sell'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                new_row['Cur..1'] = pair_currency
                add_row['Buy'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                add_row['Cur.'] = pair_currency
            else:
                comment = row.get('OG_Comment_Raw', '').strip().replace('[', '').replace(']', '')
                if 'delegated' in comment.lower():
                    comment2 = comment.split(' ')
                    if len(comment2) > 2:
                        new_row['Sell'] = pd.to_numeric(comment2[1], errors='coerce')
                        new_row['Cur..1'] = comment2[2].strip()
                        add_row['Buy'] = pd.to_numeric(comment2[1], errors='coerce')
                        add_row['Cur.'] = comment2[2].strip()
            final_rows.append(add_row)
        elif 'undelegate' in category or category == 'unlocking term deposit':
            new_row['Type'] = 'Deposit'
            maincomment = 'UNSTAKING     ' + maincomment.strip()
            add_row['Type'] = 'Withdrawal'
            add_row['Date'] = extract_datetime_combined(row.get('DateTime_Raw'))
            add_row['Comment'] = maincomment
            exchange = row.get('Exchange_Raw', platform)
            staking = exchange.lower().replace('blockchain', 'staking') if 'blockchain' in exchange.lower() else 'staking'
            add_row['Exchange'] = staking
            if (currency is not None and currency != '' and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Cur.'] = currency
                add_row['Sell'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                add_row['Cur..1'] = currency
            else:
                comment = row.get('OG_Comment_Raw', '').strip().replace('[', '').replace(']', '')
                if 'undelegated' in comment.lower():
                    comment2 = comment.split(' ')
                    if len(comment2) > 2:
                        new_row['Buy'] = pd.to_numeric(comment2[1], errors='coerce')
                        new_row['Cur.'] = comment2[2].strip()
                        add_row['Sell'] = pd.to_numeric(comment2[1], errors='coerce')
                        add_row['Cur..1'] = comment2[2].strip()
            final_rows.append(add_row)
        elif category == 'interest':
            if currency.lower() == 'usd':
                continue # Skip USD interest, as it's not crypto

            if (currency is not None and (row.get('Buy_Amount_Raw') is not None and row.get('Buy_Amount_Raw') != 0)):
                if row.get('Buy_Amount_Raw') > 0:
                    new_row['Type'] = 'Interest Income'
                    new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                    new_row['Cur.'] = currency
                elif row.get('Buy_Amount_Raw') < 0:
                    new_row['Type'] = 'Other Fee'
                    new_row['Sell'] = abs(pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce'))
                    new_row['Cur..1'] = currency
        else: # trade
            new_row['Type'] = 'Trade'
            if operation is None or operation == '' or operation == 'buy':
                # If operation is not specified, assume it's normal buy trade
                new_row['Buy'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Sell'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                if not config["consolidation_style"] == "pair":
                    new_row['Cur.'] = currency
                    new_row['Cur..1'] = pair_currency
            elif operation == 'sell':
                new_row['Sell'] = pd.to_numeric(row.get('Buy_Amount_Raw'), errors='coerce')
                new_row['Buy'] = pd.to_numeric(row.get('Sell_Amount_Raw'), errors='coerce')
                if not config["consolidation_style"] == "pair":
                    new_row['Cur..1'] = currency
                    new_row['Cur.'] = pair_currency
        new_row['Comment'] = maincomment
        final_rows.append(new_row)

    final_df = pd.DataFrame(final_rows, columns=config["target_columns"])

    # Final cleaning and sorting
    for col in ['Buy', 'Sell', 'Fee']:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

    if not final_df.empty and 'Date' in final_df.columns:
        final_df['Sort_DateTime'] = pd.to_datetime(final_df['Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        final_df = final_df.sort_values(by='Sort_DateTime', na_position='first').drop(columns=['Sort_DateTime'])
        final_df['Date'] = final_df['Date'].astype(str)

    return final_df

# --- 4. Main Controller Function ---
def process_file(input_df, config):
    """
    Processes the input DataFrame based on the consolidation style specified in the config.
    """
    style = config.get("consolidation_style")

    if style == "by_trade_id_and_time":
        print(f"Using leg-based consolidation for {config['platform_name']}...")
        # NOTE: 'consolidate_legs_to_final_df' would be your original 'consolidate_trade_rows' function
        intermediate_df = process_to_intermediate_legs(input_df, config)
        final_df = consolidate_legs_to_final_df(intermediate_df, config)
        return final_df
        
    elif style == "direct" or style == "pair":
        print(f"Using direct processing for {config['platform_name']}...")
        final_df = process_csv_direct(input_df, config)
        return final_df
        
    else:
        raise ValueError(f"Unknown consolidation_style: '{style}' in config for {config['platform_name']}.")



