import logging
import numpy as np
import pandas as pd
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from db_manager import DBManager

logger = logging.getLogger(__name__)

def get_db_api_credentials():
    db_conn = BaseHook.get_connection("pipeline_db")
    api_conn = BaseHook.get_connection("birdseye_api")
    return db_conn.get_uri(), api_conn.password  # assuming API key stored as password

DB_URL, API_KEY = get_db_api_credentials()

@task
def process_new_trader_pnl():
    """
    Process tracked swaps from the 'tracked_address_swaps' table and compute final trader statistics.
    Unrealized pnl is not computed (set to null) to avoid costly API lookups.
    Final metrics (realized pnl, win rate, trades per day, etc.) are computed and the summary is inserted
    into the 'trader_pnl' table.
    """
    db_manager = DBManager(db_url=DB_URL)
    compare_time = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S.%f')

    query = f"""
        SELECT DISTINCT ON (tx_hash) *
        FROM tracked_address_swaps
        WHERE "quote.type_swap" IS NOT NULL
            AND "quote.ui_change_amount" IS NOT NULL
            AND "quote.nearest_price" IS NOT NULL
            AND "base.type_swap" IS NOT NULL
            AND "base.ui_change_amount" IS NOT NULL
            AND "base.nearest_price" IS NOT NULL
            AND timestamp > '{compare_time}'
            AND owner NOT IN (SELECT owner FROM trader_pnl)
        ORDER BY tx_hash, timestamp DESC;
    """
    df = db_manager.execute_query(query, df=True)
    df = df.dropna(axis=1)
    logger.info(f"Extracted {df.shape[0]} rows from tracked_address_swaps.")

    # --- Step 2: Skip current price lookup ---
    # We set current_prices empty, and unrealized pnl will be np.nan.
    current_prices = {}
    logger.info("Skipping current price lookup; unrealized pnl will be set to null.")

    # --- Step 3: Process Trades to Compute PnL and Win Rate ---
    df = df.sort_values("timestamp")

    def process_trader_trades(trader_df, current_prices):
        trader_df = trader_df.sort_values('timestamp')
        portfolio = {}  # key: token address; value: dict with 'quantity', 'cost_basis', 'symbol'
        realized_pnl = 0.0
        pnl_history = []
        sale_trade_count = 0
        winning_trade_count = 0

        for idx, row in trader_df.iterrows():
            trade_time = row['timestamp']
            # Determine sale vs. purchase leg based on type_swap.
            if row['quote.type_swap'] == 'from':
                sold_token_addr = row['quote.address']
                sold_token_symbol = row['quote.symbol']
                sold_ui_change = row['quote.ui_change_amount']  # negative for sale
                if sold_ui_change >= 0:
                    logger.warning(f"Unexpected non-negative ui_change_amount for sale on row {idx}")
                    continue
                sale_qty = abs(sold_ui_change)
                sale_price = row['quote.nearest_price']
                sale_value = sale_qty * sale_price

                bought_token_addr = row['base.address']
                bought_token_symbol = row['base.symbol']
                bought_ui_change = row['base.ui_change_amount']  # positive for buy
                if bought_ui_change <= 0:
                    logger.warning(f"Unexpected non-positive ui_change_amount for buy on row {idx}")
                    continue
                buy_qty = bought_ui_change
                buy_price = row['base.nearest_price']
                acquisition_cost = buy_qty * buy_price

            elif row['quote.type_swap'] == 'to':
                sold_token_addr = row['base.address']
                sold_token_symbol = row['base.symbol']
                sold_ui_change = row['base.ui_change_amount']  # negative for sale
                if sold_ui_change >= 0:
                    logger.warning(f"Unexpected non-negative ui_change_amount for sale on row {idx}")
                    continue
                sale_qty = abs(sold_ui_change)
                sale_price = row['base.nearest_price']
                sale_value = sale_qty * sale_price

                bought_token_addr = row['quote.address']
                bought_token_symbol = row['quote.symbol']
                bought_ui_change = row['quote.ui_change_amount']  # positive for buy
                if bought_ui_change <= 0:
                    logger.warning(f"Unexpected non-positive ui_change_amount for buy on row {idx}")
                    continue
                buy_qty = bought_ui_change
                buy_price = row['quote.nearest_price']
                acquisition_cost = buy_qty * buy_price
            else:
                logger.warning(f"Unknown trade type for row {idx} at {trade_time}")
                continue

            # Process the sale leg.
            if sold_token_addr in portfolio:
                tracked_qty = portfolio[sold_token_addr]['quantity']
                tracked_cost_basis = portfolio[sold_token_addr]['cost_basis']
                if tracked_qty >= sale_qty:
                    avg_cost = tracked_cost_basis / tracked_qty
                    cost_removed = avg_cost * sale_qty
                    trade_realized = sale_value - cost_removed
                    realized_pnl += trade_realized
                    sale_trade_count += 1
                    if trade_realized > 0:
                        winning_trade_count += 1
                    new_qty = tracked_qty - sale_qty
                    new_cost_basis = tracked_cost_basis - cost_removed
                    if np.isclose(new_qty, 0):
                        del portfolio[sold_token_addr]
                    else:
                        portfolio[sold_token_addr]['quantity'] = new_qty
                        portfolio[sold_token_addr]['cost_basis'] = new_cost_basis
                else:
                    if tracked_qty > 0:
                        avg_cost = tracked_cost_basis / tracked_qty
                        cost_removed = avg_cost * tracked_qty
                        partial_realized = (tracked_qty * sale_price) - cost_removed
                        realized_pnl += partial_realized
                        sale_trade_count += 1
                        if partial_realized > 0:
                            winning_trade_count += 1
                    if sold_token_addr in portfolio:
                        del portfolio[sold_token_addr]
            else:
                # No tracked position; ignore pnl for this sale.
                pass

            # Process the purchase leg.
            if bought_token_addr in portfolio:
                portfolio[bought_token_addr]['quantity'] += buy_qty
                portfolio[bought_token_addr]['cost_basis'] += acquisition_cost
            else:
                portfolio[bought_token_addr] = {
                    'quantity': buy_qty,
                    'cost_basis': acquisition_cost,
                    'symbol': bought_token_symbol
                }

            # Set unrealized pnl to np.nan (skipping computation)
            unrealized_pnl = np.nan

            pnl_history.append({
                'timestamp': trade_time,
                'realized_pnl': realized_pnl,
                'unrealized_pnl': unrealized_pnl,
                'portfolio_snapshot': portfolio.copy()
            })

        win_rate = (winning_trade_count / sale_trade_count) if sale_trade_count > 0 else np.nan
        pnl_history_df = pd.DataFrame(pnl_history)
        return portfolio, realized_pnl, pnl_history_df, win_rate

    def process_all_traders(df, current_prices):
        traders_pnl = {}
        for owner, trader_df in df.groupby('owner'):
            logger.info(f"Processing trades for owner {owner}")
            final_portfolio, realized_pnl, pnl_history, win_rate = process_trader_trades(trader_df, current_prices)
            traders_pnl[owner] = {
                'final_portfolio': final_portfolio,
                'realized_pnl': realized_pnl,
                'pnl_history': pnl_history,
                'win_rate': win_rate
            }
        return traders_pnl

    traders_pnl_data = process_all_traders(df, current_prices)

    # Build a summary for each trader.
    trader_summary_list = []
    for owner, pnl_data in traders_pnl_data.items():
        pnl_history_df = pnl_data['pnl_history']
        if not pnl_history_df.empty:
            final_snapshot = pnl_history_df.iloc[-1]
            realized_pnl = pnl_data['realized_pnl']
            unrealized_pnl = final_snapshot['unrealized_pnl']
        else:
            realized_pnl = 0.0
            unrealized_pnl = np.nan

        total_pnl = realized_pnl if unrealized_pnl is np.nan else realized_pnl + unrealized_pnl
        win_rate = pnl_data.get('win_rate', 0.0)
        if win_rate is np.nan or pd.isna(win_rate):
            win_rate = 0.0

        trader_summary_list.append({
            'owner': owner,
            'realized_pnl': realized_pnl,
            'unrealized_pnl': unrealized_pnl,
            'total_pnl': total_pnl,
            'win_rate': win_rate
        })

    summary_df = pd.DataFrame(trader_summary_list)
    summary_df = summary_df.sort_values('total_pnl', ascending=False)

    # --- Step 4: Compute Additional Trader Statistics ---
    df['trade_timestamp'] = pd.to_datetime(df['block_unix_time'], unit='s')
    trader_time = df.groupby('owner')['trade_timestamp'].agg(['min', 'max']).reset_index()
    trader_time.rename(columns={'min': 'first_trade', 'max': 'last_trade'}, inplace=True)
    trader_time['time_window_days'] = (
        (trader_time['last_trade'] - trader_time['first_trade']).dt.total_seconds() / (3600 * 24)
    ).replace(0, 1e-6)
    trade_counts = df.groupby('owner').size().reset_index(name='trade_count')
    trader_time = pd.merge(trader_time, trade_counts, on='owner')
    trader_time['trades_per_day'] = trader_time['trade_count'] / trader_time['time_window_days']
    avg_swap = df.groupby('owner')['quote.ui_amount'].mean().reset_index()
    avg_swap.rename(columns={'quote.ui_amount': 'avg_swap_value'}, inplace=True)
    trader_time = pd.merge(trader_time, avg_swap, on='owner')

    def unique_tokens(group):
        tokens = set(group['base.address'].unique()).union(set(group['quote.address'].unique()))
        return len(tokens)
    unique_tokens_df = df.groupby('owner').apply(unique_tokens).reset_index(name='unique_coins')
    trader_time = pd.merge(trader_time, unique_tokens_df, on='owner')
    trader_time['coins_per_day'] = trader_time['unique_coins'] / trader_time['time_window_days']
    current_time = pd.Timestamp.now()
    trader_time['days_since_last_trade'] = (
        (current_time - trader_time['last_trade']).dt.total_seconds() / (3600 * 24)
    )

    # --- Step 5: Merge All Calculated Metrics ---
    final_df = pd.merge(summary_df, trader_time, on='owner', how='outer')
    final_df['inserted_timestamp'] = pd.Timestamp.now()

    # --- Step 6: Store All Results in Table "trader_pnl" ---
    db_manager.insert_raw_dataframe('trader_pnl', final_df)
    logger.info("Inserted processed data into the 'trader_pnl' table.")
    db_manager.quit()
