# balance.py

import pandas as pd

def calculate_balances(formatted_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the final balances of each currency from a standardized transaction DataFrame.

    This function iterates through each transaction, updating a dictionary that tracks the
    total amount for each currency.

    Args:
        formatted_df: The DataFrame after it has been processed and standardized.

    Returns:
        A new DataFrame summarizing the final balance of each asset.
    """
    # Use a dictionary to store the balance of each currency.
    balances = {}

    # Iterate through each transaction row in the formatted DataFrame.
    for _, row in formatted_df.iterrows():
        # Handle 'Buy' amounts (these increase a currency's balance).
        buy_cur = row.get('Cur.')
        buy_amount = row.get('Buy', 0)
        if buy_cur and pd.notna(buy_amount) and buy_amount > 0:
            # .get(key, 0) safely gets the current balance or starts at 0 if the currency is new.
            balances[buy_cur] = balances.get(buy_cur, 0) + buy_amount

        # Handle 'Sell' amounts (these decrease a currency's balance).
        sell_cur = row.get('Cur..1')
        sell_amount = row.get('Sell', 0)
        if sell_cur and pd.notna(sell_amount) and sell_amount > 0:
            balances[sell_cur] = balances.get(sell_cur, 0) - sell_amount

        # Handle 'Fee' amounts (these also decrease a currency's balance).
        fee_cur = row.get('Cur..2')
        fee_amount = row.get('Fee', 0)
        if fee_cur and pd.notna(fee_amount) and fee_amount > 0:
            balances[fee_cur] = balances.get(fee_cur, 0) - fee_amount

    if not balances:
        return pd.DataFrame(columns=['Currency', 'Final Balance'])

    # Convert the balances dictionary into a clean DataFrame for display.
    balance_df = pd.DataFrame(list(balances.items()), columns=['Currency', 'Final Balance'])
    
    # Sort the DataFrame for a clean, predictable order.
    balance_df = balance_df.sort_values(by='Final Balance', ascending=False).reset_index(drop=True)

    return balance_df