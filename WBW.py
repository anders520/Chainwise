# WBW.py by Ali, improved by Anders
import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
import traceback
import logging

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logging.info("Script started - logging initialized.")

# Set pandas options for 8-decimal precision
pd.set_option('display.float_format', '{:.8f}'.format)
logging.info("Set pandas display options.")

def load_data(closing_file_path_or_object, balance_file_path_or_object):
    logging.info("Loading closing position report.")
    logging.info("Loading balance by exchange report.")
    
    try:
        raw_closing_df = pd.read_csv(closing_file_path_or_object)
        raw_balance_df = pd.read_csv(balance_file_path_or_object)
        logging.info("Successfully loaded both CSV files.")
    except Exception as e:
        logging.error(f"Error loading CSV files: {str(e)}")
        raise
    
    closing_df = raw_closing_df.copy()
    balance_df = raw_balance_df.copy()
    
    numeric_cols = ['Amount', 'Purchase Price in USD', 'Year End Price in USD', 
                    'Cost Basis in USD', 'Year End Value in USD', 'Gain/Loss in USD']
    for col in numeric_cols:
        if col in closing_df.columns:
            closing_df[col] = closing_df[col].astype(str).str.replace('"', '').str.replace(',', '')
            closing_df[col] = pd.to_numeric(closing_df[col], errors='coerce').fillna(0)
        if col in balance_df.columns:
            balance_df[col] = balance_df[col].astype(str).str.replace('"', '').str.replace(',', '')
            balance_df[col] = pd.to_numeric(balance_df[col], errors='coerce').fillna(0)
    
    for df in [closing_df, balance_df]:
        df['Account'] = df['Account'].str.strip().str.upper()
        df['Currency'] = df['Currency'].str.strip().str.upper()
    
    if 'comments' not in closing_df.columns:
        closing_df['comments'] = ''
    
    closing_df['Date Acquired'] = pd.to_datetime(
        closing_df['Date Acquired'].astype(str).str.strip(),
        dayfirst=True, errors='coerce'
    )
    mask = closing_df['Date Acquired'].isna()
    if mask.any():
        closing_df.loc[mask, 'Date Acquired'] = pd.to_datetime(
            raw_closing_df.loc[mask, 'Date Acquired'].astype(str).str.strip(),
            dayfirst=False, errors='coerce'
        )
    
    closing_df['Calculated Cost Basis'] = closing_df['Amount'] * closing_df['Purchase Price in USD']
    inconsistencies = closing_df[abs(closing_df['Cost Basis in USD'] - closing_df['Calculated Cost Basis']) > 1e-8]
    if not inconsistencies.empty:
        logging.warning("Data inconsistencies found in closing_df:")
        logging.warning(inconsistencies[['Amount', 'Purchase Price in USD', 'Cost Basis in USD', 'Calculated Cost Basis']])
    
    return raw_closing_df, raw_balance_df, closing_df, balance_df

def calculate_discrepancies(closing_df, balance_df):
    logging.info("Calculating initial discrepancies.")
    
    closing_agg = closing_df.groupby(['Currency', 'Account'])['Amount'].sum().reset_index()
    balance_agg = balance_df.groupby(['Currency', 'Account'])['Amount'].sum().reset_index()
    
    discrepancies = balance_agg.merge(
        closing_agg,
        on=['Currency', 'Account'],
        how='outer',
        suffixes=('_balance', '_closing')
    ).fillna(0)
    discrepancies['Discrepancy'] = discrepancies['Amount_balance'] - discrepancies['Amount_closing']
    discrepancies_simple = discrepancies[['Currency', 'Account', 'Amount_balance', 'Amount_closing', 'Discrepancy']].copy()
    
    global_balance = balance_df.groupby('Currency')['Amount'].sum().reset_index()
    global_closing = closing_df.groupby('Currency')['Amount'].sum().reset_index()
    global_discrepancies = global_balance.merge(
        global_closing,
        on='Currency',
        how='outer',
        suffixes=('_balance', '_closing')
    ).fillna(0)
    global_discrepancies['Discrepancy'] = global_discrepancies['Amount_balance'] - global_discrepancies['Amount_closing']
    
    logging.info("Discrepancy calculation completed.")
    return discrepancies_simple, global_discrepancies

def reallocate_excess(closing_df, discrepancies, balance_df):
    logging.info("Starting reallocation process.")
    adjusted_df = closing_df.copy()
    reallocation_details = []
    
    for currency in discrepancies['Currency'].unique():
        curr_discrepancies = discrepancies[discrepancies['Currency'] == currency]
        excess_accounts = curr_discrepancies[curr_discrepancies['Discrepancy'] < -1e-8]['Account'].unique()
        shortage_accounts = curr_discrepancies[curr_discrepancies['Discrepancy'] > 1e-8]['Account'].unique()
        
        cex_shortages = curr_discrepancies[
            (curr_discrepancies['Discrepancy'] > 1e-8) & 
            (curr_discrepancies['Account'].isin(balance_df[balance_df['Account Type'] == 'CEX']['Account']))
        ].sort_values('Account')
        wallet_shortages = curr_discrepancies[
            (curr_discrepancies['Discrepancy'] > 1e-8) & 
            (curr_discrepancies['Account'].isin(balance_df[balance_df['Account Type'] == 'Wallet']['Account']))
        ].sort_values('Account')
        
        excess_tax_lots = adjusted_df[
            (adjusted_df['Currency'] == currency) & 
            (adjusted_df['Account'].isin(excess_accounts)) & 
            (adjusted_df['Amount'] > 0)
        ].copy()
        
        excess_limits = curr_discrepancies[curr_discrepancies['Discrepancy'] < -1e-8].set_index('Account')['Discrepancy'].to_dict()
        reallocated_amounts = {account: 0.0 for account in excess_accounts}
        
        for _, shortage in cex_shortages.iterrows():
            target_account = shortage['Account']
            shortage_amount = shortage['Discrepancy']
            if shortage_amount <= 1e-8:
                continue
            logging.info(f"Reallocating {shortage_amount:.8f} {currency} to CEX {target_account}")
            
            available_tax_lots = excess_tax_lots.sort_values(
                by=['Purchase Price in USD', 'Date Acquired', 'Amount'],
                ascending=[False, True, False]
            )
            remaining = shortage_amount
            for idx, tax_lot in available_tax_lots.iterrows():
                if remaining <= 1e-8:
                    break
                source_account = tax_lot['Account']
                if source_account not in excess_limits or reallocated_amounts[source_account] >= -excess_limits[source_account]:
                    continue
                available = tax_lot['Amount']
                max_reallocatable = -excess_limits[source_account] - reallocated_amounts[source_account]
                amount_to_reallocate = min(available, remaining, max_reallocatable)
                
                if amount_to_reallocate <= 1e-8:
                    continue
                
                new_tax_lot = tax_lot.copy()
                new_tax_lot['Account'] = target_account
                new_tax_lot['Amount'] = amount_to_reallocate
                new_tax_lot['Date Acquired'] = tax_lot['Date Acquired']
                
                if tax_lot['Purchase Price in USD'] == 0 and tax_lot['Cost Basis in USD'] != 0:
                    proportion = amount_to_reallocate / tax_lot['Amount']
                    cost_basis_to_transfer = tax_lot['Cost Basis in USD'] * proportion
                else:
                    cost_basis_to_transfer = amount_to_reallocate * tax_lot['Purchase Price in USD']
                new_tax_lot['Cost Basis in USD'] = cost_basis_to_transfer
                new_tax_lot['Year End Value in USD'] = amount_to_reallocate * tax_lot['Year End Price in USD']
                new_tax_lot['Gain/Loss in USD'] = new_tax_lot['Year End Value in USD'] - new_tax_lot['Cost Basis in USD']
                new_tax_lot['comments'] = f"Reallocated {amount_to_reallocate:.8f} {currency} from {source_account} to {target_account} (CEX rule)"
                
                adjusted_df.loc[idx, 'Amount'] -= amount_to_reallocate
                adjusted_df.loc[idx, 'Cost Basis in USD'] -= cost_basis_to_transfer
                adjusted_df.loc[idx, 'Year End Value in USD'] -= new_tax_lot['Year End Value in USD']
                adjusted_df.loc[idx, 'Gain/Loss in USD'] -= new_tax_lot['Gain/Loss in USD']
                total_cost_basis_transferred = cost_basis_to_transfer
                if adjusted_df.loc[idx, 'Amount'] <= 1e-8:
                    adjusted_df.loc[idx, 'Amount'] = 0
                    remaining_cost_basis = adjusted_df.loc[idx, 'Cost Basis in USD']
                    if remaining_cost_basis > 1e-8:
                        new_tax_lot['Cost Basis in USD'] += remaining_cost_basis
                        total_cost_basis_transferred += remaining_cost_basis
                        new_tax_lot['Gain/Loss in USD'] = new_tax_lot['Year End Value in USD'] - new_tax_lot['Cost Basis in USD']
                        adjusted_df.loc[idx, 'Cost Basis in USD'] = 0
                        adjusted_df.loc[idx, 'Gain/Loss in USD'] = 0
                    adjusted_df.loc[idx, 'comments'] = f"Exhausted {amount_to_reallocate:.8f} {currency}; originally held {tax_lot['Amount']:.8f} {currency}; reallocated to {target_account} (CEX rule)"
                else:
                    adjusted_df.loc[idx, 'comments'] = f"Partially used {amount_to_reallocate:.8f} {currency} reallocated to {target_account} (CEX rule)"
                
                adjusted_df = pd.concat([adjusted_df, pd.DataFrame([new_tax_lot])], ignore_index=True)
                reallocation_details.append({
                    'Currency': currency,
                    'Source Account': source_account,
                    'Target Account': target_account,
                    'Amount': amount_to_reallocate,
                    'Cost Basis Transferred in USD': total_cost_basis_transferred,
                    'Purchase Price in USD': tax_lot['Purchase Price in USD'],
                    'Date Acquired': tax_lot['Date Acquired'],
                    'Reason': 'Reallocation to CEX shortage',
                    'Comment': new_tax_lot['comments']
                })
                remaining -= amount_to_reallocate
                reallocated_amounts[source_account] += amount_to_reallocate
                excess_tax_lots.loc[idx, 'Amount'] -= amount_to_reallocate
        
        for _, shortage in wallet_shortages.iterrows():
            target_account = shortage['Account']
            shortage_amount = shortage['Discrepancy']
            if shortage_amount <= 1e-8:
                continue
            logging.info(f"Reallocating {shortage_amount:.8f} {currency} to Wallet {target_account}")
            
            available_tax_lots = excess_tax_lots.sort_values(
                by=['Purchase Price in USD', 'Date Acquired', 'Amount'],
                ascending=[True, False, False]
            )
            remaining = shortage_amount
            for idx, tax_lot in available_tax_lots.iterrows():
                if remaining <= 1e-8:
                    break
                source_account = tax_lot['Account']
                if source_account not in excess_limits or reallocated_amounts[source_account] >= -excess_limits[source_account]:
                    continue
                available = tax_lot['Amount']
                max_reallocatable = -excess_limits[source_account] - reallocated_amounts[source_account]
                amount_to_reallocate = min(available, remaining, max_reallocatable)
                
                if amount_to_reallocate <= 1e-8:
                    continue
                
                new_tax_lot = tax_lot.copy()
                new_tax_lot['Account'] = target_account
                new_tax_lot['Amount'] = amount_to_reallocate
                new_tax_lot['Date Acquired'] = tax_lot['Date Acquired']
                
                if tax_lot['Purchase Price in USD'] == 0 and tax_lot['Cost Basis in USD'] != 0:
                    proportion = amount_to_reallocate / tax_lot['Amount']
                    cost_basis_to_transfer = tax_lot['Cost Basis in USD'] * proportion
                else:
                    cost_basis_to_transfer = amount_to_reallocate * tax_lot['Purchase Price in USD']
                new_tax_lot['Cost Basis in USD'] = cost_basis_to_transfer
                new_tax_lot['Year End Value in USD'] = amount_to_reallocate * tax_lot['Year End Price in USD']
                new_tax_lot['Gain/Loss in USD'] = new_tax_lot['Year End Value in USD'] - new_tax_lot['Cost Basis in USD']
                new_tax_lot['comments'] = f"Reallocated {amount_to_reallocate:.8f} {currency} from {source_account} to {target_account} (Wallet rule)"
                
                adjusted_df.loc[idx, 'Amount'] -= amount_to_reallocate
                adjusted_df.loc[idx, 'Cost Basis in USD'] -= cost_basis_to_transfer
                adjusted_df.loc[idx, 'Year End Value in USD'] -= new_tax_lot['Year End Value in USD']
                adjusted_df.loc[idx, 'Gain/Loss in USD'] -= new_tax_lot['Gain/Loss in USD']
                total_cost_basis_transferred = cost_basis_to_transfer
                if adjusted_df.loc[idx, 'Amount'] <= 1e-8:
                    adjusted_df.loc[idx, 'Amount'] = 0
                    remaining_cost_basis = adjusted_df.loc[idx, 'Cost Basis in USD']
                    if remaining_cost_basis > 1e-8:
                        new_tax_lot['Cost Basis in USD'] += remaining_cost_basis
                        total_cost_basis_transferred += remaining_cost_basis
                        new_tax_lot['Gain/Loss in USD'] = new_tax_lot['Year End Value in USD'] - new_tax_lot['Cost Basis in USD']
                        adjusted_df.loc[idx, 'Cost Basis in USD'] = 0
                        adjusted_df.loc[idx, 'Gain/Loss in USD'] = 0
                    adjusted_df.loc[idx, 'comments'] = f"Exhausted {amount_to_reallocate:.8f} {currency}; originally held {tax_lot['Amount']:.8f} {currency}; reallocated to {target_account} (Wallet rule)"
                else:
                    adjusted_df.loc[idx, 'comments'] = f"Partially used {amount_to_reallocate:.8f} {currency} reallocated to {target_account} (Wallet rule)"
                
                adjusted_df = pd.concat([adjusted_df, pd.DataFrame([new_tax_lot])], ignore_index=True)
                reallocation_details.append({
                    'Currency': currency,
                    'Source Account': source_account,
                    'Target Account': target_account,
                    'Amount': amount_to_reallocate,
                    'Cost Basis Transferred in USD': total_cost_basis_transferred,
                    'Purchase Price in USD': tax_lot['Purchase Price in USD'],
                    'Date Acquired': tax_lot['Date Acquired'],
                    'Reason': 'Reallocation to Wallet shortage',
                    'Comment': new_tax_lot['comments']
                })
                remaining -= amount_to_reallocate
                reallocated_amounts[source_account] += amount_to_reallocate
                excess_tax_lots.loc[idx, 'Amount'] -= amount_to_reallocate
    
    adjusted_df['Calculated Cost Basis'] = adjusted_df['Amount'] * adjusted_df['Purchase Price in USD']
    inconsistencies = adjusted_df[abs(adjusted_df['Cost Basis in USD'] - adjusted_df['Calculated Cost Basis']) > 1e-8]
    if not inconsistencies.empty:
        logging.warning("Data inconsistencies found in adjusted_df after reallocation:")
        logging.warning(inconsistencies[['Amount', 'Purchase Price in USD', 'Cost Basis in USD', 'Calculated Cost Basis']])
    
    logging.info("Reallocation process completed.")
    return adjusted_df, pd.DataFrame(reallocation_details)

def resolve_global_adjustments(adjusted_df, global_discrepancies, balance_df):
    logging.info("Resolving global adjustments.")
    final_df = adjusted_df.copy()
    write_off_details = []
    manual_entries = []
    
    closing_agg = final_df.groupby(['Currency', 'Account'])['Amount'].sum().reset_index()
    balance_agg = balance_df.groupby(['Currency', 'Account'])['Amount'].sum().reset_index()
    
    account_discrepancies = balance_agg.merge(
        closing_agg,
        on=['Currency', 'Account'],
        how='outer',
        suffixes=('_balance', '_closing')
    ).fillna(0)
    account_discrepancies['Discrepancy'] = account_discrepancies['Amount_balance'] - account_discrepancies['Amount_closing']
    balanced_accounts = account_discrepancies[abs(account_discrepancies['Discrepancy']) <= 1e-8][['Currency', 'Account']]
    
    for _, global_row in global_discrepancies.iterrows():
        currency = global_row['Currency']
        global_discrepancy = global_row['Discrepancy']
        logging.info(f"Processing global discrepancy for {currency}: {global_discrepancy:.8f}")
        
        if global_discrepancy > 1e-8:
            account_discrepancies = balance_agg.merge(
                closing_agg,
                on=['Currency', 'Account'],
                how='outer',
                suffixes=('_balance', '_closing')
            ).fillna(0)
            account_discrepancies['Discrepancy'] = account_discrepancies['Amount_balance'] - account_discrepancies['Amount_closing']
            shortage_accounts = account_discrepancies[
                (account_discrepancies['Currency'] == currency) & 
                (account_discrepancies['Discrepancy'] > 1e-8) &
                (~account_discrepancies[['Currency', 'Account']].apply(tuple, axis=1).isin(
                    balanced_accounts[['Currency', 'Account']].apply(tuple, axis=1)
                ))
            ].sort_values('Account')
            
            if not shortage_accounts.empty:
                total_shortage = shortage_accounts['Discrepancy'].sum()
                proportion = global_discrepancy / total_shortage if total_shortage > 0 else 1
                for _, account_row in shortage_accounts.iterrows():
                    account = account_row['Account']
                    account_shortage = account_row['Discrepancy']
                    amount_to_add = min(account_shortage, global_discrepancy * proportion)
                    if amount_to_add <= 1e-8:
                        continue
                    logging.info(f"Adding manual entry of {amount_to_add:.8f} {currency} to {account}")
                    
                    year_end_price = final_df[final_df['Currency'] == currency]['Year End Price in USD'].iloc[0] if not final_df[final_df['Currency'] == currency].empty else 0
                    account_type = balance_df[balance_df['Account'] == account]['Account Type'].iloc[0] if not balance_df[balance_df['Account'] == account].empty else 'Unknown'
                    
                    manual_entry = {
                        'Amount': amount_to_add,
                        'Currency': currency,
                        'Date Acquired': pd.to_datetime('31/12/2024', dayfirst=True),
                        'Account': account,
                        'Account Type': account_type,
                        'Purchase Price in USD': 0.0,
                        'Year End Price in USD': year_end_price,
                        'Cost Basis in USD': 0.0,
                        'Year End Value in USD': amount_to_add * year_end_price,
                        'Gain/Loss in USD': amount_to_add * year_end_price,
                        'comments': f"Manual zero-basis entry of {amount_to_add:.8f} {currency} added to resolve global shortage"
                    }
                    final_df = pd.concat([final_df, pd.DataFrame([manual_entry])], ignore_index=True)
                    manual_entries.append(manual_entry)
        
        elif global_discrepancy < -1e-8:
            account_discrepancies = balance_agg.merge(
                closing_agg,
                on=['Currency', 'Account'],
                how='outer',
                suffixes=('_balance', '_closing')
            ).fillna(0)
            account_discrepancies['Discrepancy'] = account_discrepancies['Amount_balance'] - account_discrepancies['Amount_closing']
            excess_accounts = account_discrepancies
            excess_accounts = excess_accounts[
                (excess_accounts['Currency'] == currency) & 
                (excess_accounts['Discrepancy'] < -1e-8)
            ].sort_values('Account')
            
            if not excess_accounts.empty:
                total_excess = -excess_accounts['Discrepancy'].sum()
                proportion = -global_discrepancy / total_excess if total_excess > 0 else 1
                for _, account_row in excess_accounts.iterrows():
                    account = account_row['Account']
                    account_excess = -account_row['Discrepancy']
                    amount_to_write_off = min(account_excess, -global_discrepancy * proportion)
                    if amount_to_write_off <= 1e-8:
                        continue
                    logging.info(f"Writing off {amount_to_write_off:.8f} {currency} from {account}")
                    
                    tax_lots = final_df[
                        (final_df['Currency'] == currency) & 
                        (final_df['Account'] == account) & 
                        (final_df['Amount'] > 0)
                    ].sort_values(['Purchase Price in USD', 'Date Acquired', 'Amount'], ascending=[True, False, False])
                    
                    remaining = amount_to_write_off
                    for idx, tax_lot in tax_lots.iterrows():
                        if remaining <= 1e-8:
                            break
                        write_off_amount = min(tax_lot['Amount'], remaining)
                        cost_basis_to_write_off = 0.0
                        if tax_lot['Purchase Price in USD'] == 0 and tax_lot['Cost Basis in USD'] != 0:
                            proportion = write_off_amount / tax_lot['Amount']
                            cost_basis_to_write_off = tax_lot['Cost Basis in USD'] * proportion
                        else:
                            cost_basis_to_write_off = write_off_amount * tax_lot['Purchase Price in USD']
                        final_df.loc[idx, 'Amount'] -= write_off_amount
                        final_df.loc[idx, 'Cost Basis in USD'] -= cost_basis_to_write_off
                        final_df.loc[idx, 'Year End Value in USD'] -= write_off_amount * tax_lot['Year End Price in USD']
                        final_df.loc[idx, 'Gain/Loss in USD'] -= write_off_amount * (tax_lot['Year End Price in USD'] - tax_lot['Purchase Price in USD'])
                        if final_df.loc[idx, 'Amount'] <= 1e-8:
                            final_df.loc[idx, 'Amount'] = 0
                            remaining_cost_basis = final_df.loc[idx, 'Cost Basis in USD']
                            if remaining_cost_basis > 1e-8:
                                cost_basis_to_write_off += remaining_cost_basis
                                final_df.loc[idx, 'Cost Basis in USD'] = 0
                                final_df.loc[idx, 'Gain/Loss in USD'] = 0
                            final_df.loc[idx, 'comments'] = f"Exhausted {write_off_amount:.8f} {currency}; originally held {tax_lot['Amount']:.8f} {currency}; written off to match balance by exchange"
                        else:
                            final_df.loc[idx, 'comments'] = f"Wrote off {write_off_amount:.8f} {currency} to match balance by exchange"
                        write_off_details.append({
                            'Currency': currency,
                            'Account': account,
                            'Amount Written Off': write_off_amount,
                            'Cost Basis Written Off in USD': cost_basis_to_write_off,
                            'Reason': 'Excess adjustment to match balance by exchange'
                        })
                        remaining -= write_off_amount
    
    final_df['Calculated Cost Basis'] = final_df['Amount'] * final_df['Purchase Price in USD']
    inconsistencies = final_df[abs(final_df['Cost Basis in USD'] - final_df['Calculated Cost Basis']) > 1e-8]
    if not inconsistencies.empty:
        logging.warning("Data inconsistencies found in final_df after global adjustments:")
        logging.warning(inconsistencies[['Amount', 'Purchase Price in USD', 'Cost Basis in USD', 'Calculated Cost Basis']])
    
    logging.info("Global adjustments completed.")
    return final_df, pd.DataFrame(write_off_details), pd.DataFrame(manual_entries)

def add_comments(final_df, discrepancies):
    logging.info("Adding comments to adjusted closing position.")
    for _, row in discrepancies.iterrows():
        currency, account = row['Currency'], row['Account']
        if abs(row['Discrepancy']) < 1e-8:
            final_df.loc[
                (final_df['Currency'] == currency) & 
                (final_df['Account'] == account) & 
                (final_df['comments'] == ''),
                'comments'
            ] = f"No discrepancy; balance matches balance by exchange"
        elif final_df[(final_df['Currency'] == currency) & (final_df['Account'] == account)].empty:
            final_df.loc[
                (final_df['Currency'] == currency) & 
                (final_df['Account'] == account) & 
                (final_df['comments'] == ''),
                'comments'
            ] = f"No tax lots used; not present in adjusted closing position"
    logging.info("Comments added.")
    return final_df

def generate_cost_basis_summary(original_df, adjusted_df, write_off_details):
    logging.info("Generating cost basis summary (requested columns).")
    pre_amount = original_df.groupby('Currency', as_index=False)['Amount'].sum() \
        .rename(columns={'Amount': 'Total Amount Before Write-Off'})
    pre_basis = original_df.groupby('Currency', as_index=False)['Cost Basis in USD'].sum() \
        .rename(columns={'Cost Basis in USD': 'Total Cost Basis Before Write-Off (USD)'})
    pre = pre_amount.merge(pre_basis, on='Currency', how='outer').fillna(0)
    if write_off_details is not None and not write_off_details.empty:
        wo_amt = write_off_details.groupby('Currency', as_index=False)['Amount Written Off'].sum() \
            .rename(columns={'Amount Written Off': 'Total Amount Written Off'})
        wo_basis = write_off_details.groupby('Currency', as_index=False)['Cost Basis Written Off in USD'].sum() \
            .rename(columns={'Cost Basis Written Off in USD': 'Total Cost Basis Written Off (USD)'})
        written_off = wo_amt.merge(wo_basis, on='Currency', how='outer').fillna(0)
    else:
        written_off = pd.DataFrame(columns=['Currency', 'Total Amount Written Off', 'Total Cost Basis Written Off (USD)'])
    post_amount = adjusted_df.groupby('Currency', as_index=False)['Amount'].sum() \
        .rename(columns={'Amount': 'Adjusted Total Amount'})
    post_basis = adjusted_df.groupby('Currency', as_index=False)['Cost Basis in USD'].sum() \
        .rename(columns={'Cost Basis in USD': 'Adjusted Total Cost Basis (USD)'})
    post = post_amount.merge(post_basis, on='Currency', how='outer').fillna(0)
    df = (pre.merge(written_off, on='Currency', how='left')
          .merge(post, on='Currency', how='left')
          .fillna(0))
    cols = [
        'Currency',
        'Total Amount Before Write-Off',
        'Total Cost Basis Before Write-Off (USD)',
        'Total Amount Written Off',
        'Total Cost Basis Written Off (USD)',
        'Adjusted Total Amount',
        'Adjusted Total Cost Basis (USD)'
    ]
    df = df[cols].sort_values('Currency').reset_index(drop=True)
    logging.info("Cost basis summary generated (requested columns).")
    return df

def save_combined_report(output_path, raw_balance_df, raw_closing_df, discrepancies_simple, global_discrepancies, adjusted_df, cost_basis_summary, write_off_details, reallocation_details, manual_entries, original_df):
    logging.info("Saving combined workbook.")
    original_cost_basis_tmp = adjusted_df['Cost Basis in USD'].sum()
    if (adjusted_df['Cost Basis in USD'] < 0).any():
        adjusted_df.loc[adjusted_df['Cost Basis in USD'] < 0, 'Cost Basis in USD'] = 0
        logging.warning("Negative cost basis values detected and corrected to 0.")
    zero_amount_with_cost = adjusted_df[(adjusted_df['Amount'] <= 1e-8) & (abs(adjusted_df['Cost Basis in USD']) > 1e-8)]
    if not zero_amount_with_cost.empty:
        logging.warning(f"Zero-amount entries with significant cost basis detected: {zero_amount_with_cost[['Amount', 'Cost Basis in USD']]}")
        adjusted_df.loc[(adjusted_df['Amount'] <= 1e-8) & (abs(adjusted_df['Cost Basis in USD']) > 1e-8), 'Cost Basis in USD'] = 0
        adjusted_df.loc[(adjusted_df['Amount'] <= 1e-8) & (abs(adjusted_df['Cost Basis in USD']) > 1e-8), 'Gain/Loss in USD'] = 0
    adjusted_df['Gain/Loss in USD'] = adjusted_df['Year End Value in USD'] - adjusted_df['Cost Basis in USD']
    new_cost_basis_tmp = adjusted_df['Cost Basis in USD'].sum()
    logging.info(f"Cost basis validation - Original: {original_cost_basis_tmp:.8f}, New: {new_cost_basis_tmp:.8f}, Difference: {(new_cost_basis_tmp - original_cost_basis_tmp):.8f}")
    original_total_cost_basis = original_df['Cost Basis in USD'].sum()
    write_off_total_cost_basis = write_off_details['Cost Basis Written Off in USD'].sum() if not write_off_details.empty else 0.0
    adjusted_total_cost_basis = adjusted_df['Cost Basis in USD'].sum()
    summary_top = pd.DataFrame({
        'Metric': ['Original Total Cost Basis', 'Total Cost Basis Written Off', 'Adjusted Total Cost Basis'],
        'Amount (USD)': [original_total_cost_basis, write_off_total_cost_basis, adjusted_total_cost_basis]
    })
    original_by_token = original_df.groupby('Currency', as_index=False)['Cost Basis in USD'].sum() \
        .rename(columns={'Cost Basis in USD': 'Original Cost Basis (USD)'})
    written_off_by_token = (write_off_details.groupby('Currency', as_index=False)['Cost Basis Written Off in USD'].sum() if not write_off_details.empty else pd.DataFrame(columns=['Currency', 'Cost Basis Written Off in USD'])) \
        .rename(columns={'Cost Basis Written Off in USD': 'Written Off (USD)'})
    adjusted_by_token = adjusted_df.groupby('Currency', as_index=False)['Cost Basis in USD'].sum() \
        .rename(columns={'Cost Basis in USD': 'Adjusted Cost Basis (USD)'})
    if manual_entries is not None and not manual_entries.empty:
        manual_added_by_token = manual_entries.groupby('Currency', as_index=False)['Amount'].sum() \
            .rename(columns={'Amount': 'Manual Added (Amount)'})
    else:
        manual_added_by_token = pd.DataFrame(columns=['Currency', 'Manual Added (Amount)'])
    summary_dist = (original_by_token
                    .merge(written_off_by_token, on='Currency', how='left')
                    .merge(adjusted_by_token, on='Currency', how='left')
                    .merge(manual_added_by_token, on='Currency', how='left')
                    .fillna(0))
    summary_dist['Changes'] = summary_dist[['Written Off (USD)', 'Adjusted Cost Basis (USD)', 'Manual Added (Amount)']].sum(axis=1)
    summary_dist = summary_dist[summary_dist['Changes'] > 1e-8].drop('Changes', axis=1)
    
    output_file = os.path.join(output_path, "Combined Report.xlsx")
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        raw_closing_df.to_excel(writer, sheet_name="Original Closing Position", index=False)
        raw_balance_df.to_excel(writer, sheet_name="Original Balance by Exchange", index=False)
        adjusted_df.to_excel(writer, sheet_name="Adjusted Closing Position", index=False)
        discrepancies_simple.to_excel(writer, sheet_name="Account-Level Discrepancies", index=False)
        global_discrepancies.to_excel(writer, sheet_name="Global Discrepancies", index=False)
        cost_basis_summary.to_excel(writer, sheet_name="Cost Basis Summary", index=False)
        if not write_off_details.empty:
            write_off_details.to_excel(writer, sheet_name="Write-Off Details", index=False)
        if not reallocation_details.empty:
            reallocation_details.to_excel(writer, sheet_name="Reallocation Details", index=False)
        if not manual_entries.empty:
            manual_entries.to_excel(writer, sheet_name="Manual Entries", index=False)
        summary_top.to_excel(writer, sheet_name="Summary", index=False)
        summary_dist.to_excel(writer, sheet_name="Summary", startrow=len(summary_top) + 2, index=False)
    logging.info(f"Combined report saved to {output_file}")


def generate_final_adjusted_closing_report(output_path, adjusted_df):
    logging.info("Generating Final Adjusted Closing Position.csv.")
    adjusted_df.to_csv(os.path.join(output_path, "Updated Closing Position Report.csv"), index=False)
    return adjusted_df

def generate_tax_lot_consolidation_details(output_path, adjusted_df):
    logging.info("Generating Tax Lot Consolidation Details.xlsx.")
    df = adjusted_df[adjusted_df['comments'].str.contains("reallocated|written off|Manual")].copy()
    if not df.empty:
        df.to_excel(os.path.join(output_path, "Tax Lot Consolidation Details.xlsx"), index=False)
    else:
        logging.info("No tax lot consolidations to report.")

def generate_cost_basis_change_analysis(output_path, adjusted_df, final_adjusted_df):
    logging.info("Generating Cost Basis Change Analysis.xlsx.")
    if final_adjusted_df.empty:
        logging.warning("Final adjusted DataFrame is empty. Skipping Cost Basis Change Analysis.")
        return
    df_adjusted = adjusted_df[['Currency', 'Account', 'Amount', 'Cost Basis in USD']].copy()
    df_final = final_adjusted_df[['Currency', 'Account', 'Amount', 'Cost Basis in USD']].copy()
    merged_df = pd.merge(df_adjusted, df_final, on=['Currency', 'Account'], suffixes=('_adj', '_final'))
    merged_df['Amount Change'] = merged_df['Amount_final'] - merged_df['Amount_adj']
    merged_df['Cost Basis Change'] = merged_df['Cost Basis in USD_final'] - merged_df['Cost Basis in USD_adj']
    changes_df = merged_df[
        (merged_df['Amount Change'].abs() > 1e-8) | 
        (merged_df['Cost Basis Change'].abs() > 1e-8)
    ]
    if not changes_df.empty:
        changes_df.to_excel(os.path.join(output_path, "Cost Basis Change Analysis.xlsx"), index=False)
    else:
        logging.info("No significant cost basis changes to report.")

def generate_cointracking_import_file(output_path, final_adjusted_df, raw_closing_df, raw_balance_df):
    logging.info("Generating CoinTracking Import File.csv.")
    
    if final_adjusted_df.empty:
        logging.warning("Final adjusted DataFrame is empty. Skipping CoinTracking import file generation.")
        return
    
    df = final_adjusted_df.copy()
    
    df = df[df['Amount'] > 0]
    
    original_accounts = pd.concat([raw_closing_df['Account'], raw_balance_df['Account']]).unique()
    df = df[df['Account'].isin(original_accounts)]
    
    cointracking_df = df.rename(columns={
        'Date Acquired': 'Date',
        'Currency': 'Buy Cur.',
        'Amount': 'Buy Amount',
        'Purchase Price in USD': 'Buy Price',
        'Account': 'Exchange (optional)'
    })
    
    cointracking_df['Type'] = 'Deposit'
    cointracking_df['Sell Cur.'] = 'USD'
    cointracking_df['Sell Amount'] = cointracking_df['Cost Basis in USD']
    cointracking_df['Transaction Type'] = 'Trade'
    cointracking_df['Comment'] = 'Auto-generated from WBW'
    
    final_cols = [
        'Type', 'Date', 'Buy Cur.', 'Buy Amount', 'Sell Cur.', 'Sell Amount',
        'Exchange (optional)', 'Transaction Type', 'Comment'
    ]
    cointracking_df = cointracking_df[final_cols]
    
    cointracking_df.to_csv(os.path.join(output_path, "CoinTracking Import File.csv"), index=False)

def main(closing_file_object, balance_file_object, output_path):
    logging.info("Starting main process.")
    try:
        raw_closing_df, raw_balance_df, closing_df, balance_df = load_data(closing_file_object, balance_file_object)
        
        discrepancies_simple, global_discrepancies = calculate_discrepancies(closing_df, balance_df)
        
        adjusted_df, reallocation_details = reallocate_excess(closing_df, discrepancies_simple, balance_df)
        
        final_adjusted_df, write_off_details, manual_entries = resolve_global_adjustments(adjusted_df, global_discrepancies, balance_df)
        
        final_adjusted_df = add_comments(final_adjusted_df, discrepancies_simple)
        
        _validate_all_dates(final_adjusted_df)
        
        cost_basis_summary = generate_cost_basis_summary(closing_df, final_adjusted_df, write_off_details)

        save_combined_report(
            output_path,
            raw_balance_df,
            raw_closing_df,
            discrepancies_simple,
            global_discrepancies,
            final_adjusted_df,
            cost_basis_summary,
            write_off_details,
            reallocation_details,
            manual_entries,
            closing_df
        )
        
        final_adjusted_df_from_report = generate_final_adjusted_closing_report(output_path, final_adjusted_df)
        generate_tax_lot_consolidation_details(output_path, final_adjusted_df)
        generate_cost_basis_change_analysis(output_path, adjusted_df, final_adjusted_df_from_report)
        generate_cointracking_import_file(output_path, final_adjusted_df_from_report, raw_closing_df, raw_balance_df)

        combined_report_path = os.path.join(output_path, "Combined Report.xlsx")
        adjusted_closing_path = os.path.join(output_path, "Updated Closing Position Report 2024.csv")
        
        logging.info("Script execution completed.")
        return combined_report_path, adjusted_closing_path, None

    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.error(f"Main process failed: {str(e)}")
        logging.error(error_traceback)
        return None, None, error_traceback


def _validate_all_dates(df):
    if not df.empty:
        invalid_dates = df['Date Acquired'].isna()
        if invalid_dates.any():
            logging.warning("Invalid dates found after processing. These rows may have been skipped or need manual correction.")
            logging.warning(df[invalid_dates])