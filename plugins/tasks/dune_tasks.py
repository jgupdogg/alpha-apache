from airflow.decorators import task
from datetime import datetime, timedelta
import logging
import pandas as pd

# Import the DuneHook from plugins
from hooks.dune_hook import DuneHook
from db_manager import DBManager

logger = logging.getLogger("token_pnl_processor")

def get_trending_token_addresses():
    """Get list of trending token addresses from database"""
    db = DBManager(db_name='solana_insiders_bronze')

    try:
        query = """
        SELECT token_address
        FROM watchlist
        WHERE (
            (m5_change > 30) OR
            (h1_change > 30) OR
            (h6_change > 30) OR
            (h24_change > 30)
        )
        AND scraped_at = (SELECT MAX(scraped_at) FROM watchlist)
        ORDER BY 
            GREATEST(
                COALESCE(m5_change, 0), 
                COALESCE(h1_change, 0), 
                COALESCE(h6_change, 0), 
                COALESCE(h24_change, 0)
            ) DESC
        """
        
        trending_tokens = db.execute_query(query)
        token_addresses = [token['token_address'] for token in trending_tokens]
        return token_addresses
    
    except Exception as e:
        logger.error(f"Error fetching trending tokens: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    finally:
        db.quit()

@task
def process_trending_tokens(min_usd: float = 1000, num_traders: int = 10, 
                          num_trader_txns: int = 20, timeout: int = 120, 
                          db_name: str = 'solana_insiders_silver',
                          table_name: str = 'recent_token_seller_pnl') -> None:
    """Get PNL analysis for sellers of trending tokens and save to database"""
    
    # Get trending token addresses
    token_addresses = get_trending_token_addresses()
    logger.info(f"Retrieved {len(token_addresses)} trending tokens to analyze")
    
    if not token_addresses:
        logger.warning("No trending tokens found to analyze")
        return
    
    # Initialize Dune hook and DB manager
    dune_hook = DuneHook()
    db_manager = DBManager(db_name=db_name)
    total_tokens_processed = 0
    
    try:
        # Get current time and create a window (past 6 hours)
        end_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        start_time = (datetime.utcnow() - timedelta(hours=6)).strftime('%Y-%m-%d %H:%M')
        
        # Loop through each token and process
        for i, token_address in enumerate(token_addresses):
            logger.info(f"Processing token {i+1}/{len(token_addresses)}: {token_address}")
            
            try:
                # Execute query for this token
                df = dune_hook.get_recent_token_seller_pnl(
                    token_address=token_address.lower(),
                    start_time=start_time,
                    end_time=end_time,
                    min_usd=min_usd,
                    num_traders=num_traders,
                    num_trader_txns=num_trader_txns,
                    timeout=timeout
                )
                
                # Check if we have results
                if df.empty:
                    logger.info(f"No seller PNL data found for token {token_address}")
                    continue
                
                logger.info(f"Found PNL data for {len(df)} traders for token {token_address}")
                        
                # Add metadata columns
                df['chain'] = 'solana'
                df['fetched_at'] = datetime.utcnow()
                
                # Save to database
                db_manager.insert_raw_dataframe(table_name, df)
                total_tokens_processed += 1
                logger.info(f"Successfully saved data for token {token_address}")
                
            except Exception as e:
                logger.error(f"Error processing token {token_address}: {e}")
                continue
        
        logger.info(f"Completed processing {total_tokens_processed}/{len(token_addresses)} tokens")
        
    except Exception as e:
        logger.error(f"Error in token processing loop: {e}")
    finally:
        # Clean up
        db_manager.quit()
        
        

def get_successful_trader_addresses():
    """Get list of successful trader addresses from database"""
    db = DBManager(db_name='solana_insiders_silver')

    try:
        query = """
        SELECT DISTINCT wallet as trader_address
        FROM recent_token_seller_pnl
        WHERE fetched_at >= (CURRENT_TIMESTAMP - INTERVAL '6 hours')
        AND trades_per_day < 5
        AND realized_pnl > 0
        """
        
        successful_traders = db.execute_query(query)
        trader_addresses = [trader['trader_address'] for trader in successful_traders]
        
        logger.info(f"Retrieved {len(trader_addresses)} successful traders")
        return trader_addresses
    
    except Exception as e:
        logger.error(f"Error fetching successful traders: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []
    finally:
        db.quit()

@task
def process_trader_pnl(num_days: int = 30, timeout: int = 180, 
                       db_name: str = 'solana_insiders_silver',
                       table_name: str = 'successful_trader_pnl') -> None:
    """Get detailed PNL analysis for successful traders and save to database"""
    
    # Get successful trader addresses
    trader_addresses = get_successful_trader_addresses()
    logger.info(f"Retrieved {len(trader_addresses)} successful traders to analyze")
    
    if not trader_addresses:
        logger.warning("No successful traders found to analyze")
        return
    
    # Initialize Dune hook and DB manager
    dune_hook = DuneHook()
    db_manager = DBManager(db_name=db_name)
    
    try:
        # Process traders in batches to avoid query size limitations
        batch_size = 50
        total_traders_processed = 0
        
        for i in range(0, len(trader_addresses), batch_size):
            batch = trader_addresses[i:i+batch_size]
            logger.info(f"Processing trader batch {i//batch_size + 1}, size: {len(batch)}")
            
            # Format addresses for the query parameter (comma-separated string)
            trader_addresses_param = "'" + "','".join([addr.strip() for addr in batch]) + "'"
            
            try:
                # Execute query for this batch of traders
                df = dune_hook.get_trader_pnl(
                    trader_addresses=trader_addresses_param,
                    num_days=num_days,
                    timeout=timeout
                )
                
                # Check if we have results
                if df.empty:
                    logger.info(f"No PNL data found for trader batch {i//batch_size + 1}")
                    continue
                
                logger.info(f"Found PNL data for {len(df)} traders in batch {i//batch_size + 1}")
                        
                # Add metadata columns
                df['chain'] = 'solana'
                df['fetched_at'] = datetime.utcnow()
                
                # Save to database
                db_manager.insert_raw_dataframe(table_name, df)
                total_traders_processed += len(df)
                logger.info(f"Successfully saved data for batch {i//batch_size + 1}")
                
            except Exception as e:
                logger.error(f"Error processing trader batch {i//batch_size + 1}: {e}")
                continue
        
        logger.info(f"Completed processing {total_traders_processed}/{len(trader_addresses)} traders")
        
    except Exception as e:
        logger.error(f"Error in trader processing loop: {e}")
    finally:
        # Clean up
        db_manager.quit()