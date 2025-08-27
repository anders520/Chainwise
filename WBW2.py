# WBW2.py by Ali, improved by Anders
import os
import sys
import logging
import traceback
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ct_vs_closing_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.info("Script started - logging initialized.")

pd.set_option('display.float_format', '{:.8f}'.format)


def _clean_numeric(series):
    return pd.to_numeric(
        series.astype(str).str.replace('"', '').str.replace(',', ''),
        errors='coerce'
    ).fillna(0.0)


def load_closing_csv(closing_file_path_or_object):
    logging.info(f"Loading Updated Closing Position report.")
    raw = pd.read_csv(closing_file_path_or_object)
    
    cols_map = {c.lower(): c for c in raw.columns}
    col_currency = cols_map.get('currency', 'Currency')
    col_account_like = None
    for k, v in cols_map.items():
        if k.replace(' ', '') in ('yearendholding', 'yearendholding', 'yearendholding'):
            col_account_like = v
            break
    if col_account_like is None:
        col_account_like = cols_map.get('account', 'Account')

    col_amount = cols_map.get('amount', 'Amount')
    col_cb = None
    for k, v in cols_map.items():
        if 'cost basis in usd' in k:
            col_cb = v
            break
    if col_cb is None:
        raise KeyError("Could not find 'Cost Basis in USD' column in closing report.")

    work = raw.copy()
    missing = [x for x in [col_currency, col_account_like, col_amount, col_cb] if x not in work.columns]
    if missing:
        raise KeyError(f"Closing report missing columns: {missing}")

    work[col_amount] = _clean_numeric(work[col_amount])
    work[col_cb] = _clean_numeric(work[col_cb])

    work['_Currency'] = work[col_currency].astype(str).str.strip()
    work['_Account'] = work[col_account_like].astype(str).str.strip()

    closing_agg = (
        work.groupby(['_Currency', '_Account'], dropna=False)
            .agg(**{
                'Amount (Closing)': (col_amount, 'sum'),
                'Cost Basis (Closing)': (col_cb, 'sum')
            })
            .reset_index()
            .rename(columns={'_Currency': 'Currency', '_Account': 'Account'})
    )

    return raw, closing_agg


def load_cointracking_csv(ct_file_path_or_object):
    logging.info(f"Loading CoinTracking import from.")
    raw = pd.read_csv(ct_file_path_or_object)

    cols_map = {c.lower(): c for c in raw.columns}
    col_buy_amt = cols_map.get('buy amount', 'Buy Amount')
    col_buy_cur = cols_map.get('buy cur.', 'Buy Cur.')
    col_sell_amt = cols_map.get('sell amount', 'Sell Amount')
    col_sell_cur = cols_map.get('sell cur.', 'Sell Cur.')
    col_exchange = cols_map.get('exchange (optional)', 'Exchange (optional)')

    missing = [x for x in [col_buy_amt, col_buy_cur, col_sell_amt, col_sell_cur, col_exchange] if x not in raw.columns]
    if missing:
        raise KeyError(f"CoinTracking file missing columns: {missing}")

    work = raw.copy()
    work[col_buy_amt] = _clean_numeric(work[col_buy_amt])
    work[col_sell_amt] = _clean_numeric(work[col_sell_amt])

    work['_Currency'] = work[col_buy_cur].astype(str).str.strip()
    work['_Account'] = work[col_exchange].astype(str).str.strip()

    ct_agg = (
        work.groupby(['_Currency', '_Account'], dropna=False)
            .agg(**{
                'Amount (CT)': (col_buy_amt, 'sum'),
                'Cost Basis (CT)': (col_sell_amt, 'sum')
            })
            .reset_index()
            .rename(columns={'_Currency': 'Currency', '_Account': 'Account'})
    )

    return raw, ct_agg


def build_detailed_comparison(closing_agg, ct_agg):
    comp = pd.merge(
        closing_agg,
        ct_agg,
        on=['Currency', 'Account'],
        how='outer'
    ).fillna(0.0)

    for col in ['Amount (Closing)', 'Cost Basis (Closing)', 'Amount (CT)', 'Cost Basis (CT)']:
        comp[col] = pd.to_numeric(comp[col], errors='coerce').fillna(0.0)

    comp['Discrepancy (Amount)'] = comp['Amount (Closing)'] - comp['Amount (CT)']
    comp['Discrepancy (Cost Basis)'] = comp['Cost Basis (Closing)'] - comp['Cost Basis (CT)']

    for col in ['Discrepancy (Amount)', 'Discrepancy (Cost Basis)']:
        comp[col] = comp[col].round(8)

    return comp


def build_global_comparison(detailed_comp):
    return (
        detailed_comp.groupby('Currency', dropna=False)
                     .agg(
                         **{
                             'Amount (Closing)': ('Amount (Closing)', 'sum'),
                             'Amount (CT)': ('Amount (CT)', 'sum'),
                             'Discrepancy (Amount)': ('Discrepancy (Amount)', 'sum'),
                             'Cost Basis (Closing)': ('Cost Basis (Closing)', 'sum'),
                             'Cost Basis (CT)': ('Cost Basis (CT)', 'sum'),
                             'Discrepancy (Cost Basis)': ('Discrepancy (Cost Basis)', 'sum')
                         }
                     )
                     .reset_index()
    )


def build_cost_basis_summary(closing_agg, ct_agg):
    summary = []
    
    total_closing_cb = closing_agg['Cost Basis (Closing)'].sum()
    total_ct_cb = ct_agg['Cost Basis (CT)'].sum()
    total_cb_disc = total_closing_cb - total_ct_cb
    summary.append({'Category': 'Total Cost Basis (Closing)', 'Amount': total_closing_cb})
    summary.append({'Category': 'Total Cost Basis (CT)', 'Amount': total_ct_cb})
    summary.append({'Category': 'Total Discrepancy (Cost Basis)', 'Amount': total_cb_disc})
    
    cb_by_cur = pd.merge(
        closing_agg.groupby('Currency')['Cost Basis (Closing)'].sum().reset_index(),
        ct_agg.groupby('Currency')['Cost Basis (CT)'].sum().reset_index(),
        on='Currency',
        how='outer'
    ).fillna(0)
    
    cb_by_cur['Discrepancy'] = cb_by_cur['Cost Basis (Closing)'] - cb_by_cur['Cost Basis (CT)']
    
    cb_by_cur = cb_by_cur.sort_values(by=['Currency']).reset_index(drop=True)
    
    summary_df = pd.DataFrame(summary)
    summary_df = pd.concat([summary_df, pd.DataFrame(columns=['Category', 'Amount'])], ignore_index=True)
    summary_df = pd.concat([summary_df, cb_by_cur.rename(columns={'Cost Basis (Closing)': 'Closing Cost Basis (USD)', 'Cost Basis (CT)': 'CT Cost Basis (USD)', 'Discrepancy': 'Discrepancy (Cost Basis)'})], ignore_index=True)
    
    return summary_df


def main(closing_file_object, ct_file_object, output_path):
    logging.info("Starting main process for WBW2.")
    try:
        out_xlsx = os.path.join(output_path, "New Closing Position vs CoinTracking Import.xlsx")

        raw_closing, closing_agg = load_closing_csv(closing_file_object)
        raw_ct, ct_agg = load_cointracking_csv(ct_file_object)

        detailed = build_detailed_comparison(closing_agg, ct_agg)

        global_comp = build_global_comparison(detailed)

        cb_summary = build_cost_basis_summary(closing_agg, ct_agg)

        with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
            raw_closing.to_excel(writer, sheet_name="Updated Closing Position", index=False)
            raw_ct.to_excel(writer, sheet_name="CoinTracking Import", index=False)
            detailed.to_excel(writer, sheet_name="Comparison", index=False)
            global_comp.to_excel(writer, sheet_name="Global Comparison", index=False)
            cb_summary.to_excel(writer, sheet_name="Cost Basis Summary", index=False)
        logging.info(f"Comparison workbook saved to {out_xlsx}")
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        logging.error(traceback.format_exc())
        return None

    return out_xlsx