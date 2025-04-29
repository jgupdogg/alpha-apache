
# Example Airflow task usage
from airflow.decorators import task
from utils.nodriver_scraper import scrape_api_data_sync


@task(pool='scraping_pool', task_id='scrape_gmgn')
def task_scrape_gmgn():
    """GMGN scraper task"""
    _, df = scrape_api_data_sync(
        url="https://gmgn.ai/trade/hyQkYMVH?chain=sol&tab=smart_degen",
        url_pattern=r'gmgn\.ai/defi/quotation/v1/rank/sol/wallets',
        timeout=20
    )
    
    if df is not None:
        # Store DataFrame in database
        from db_manager import DBManager
        db_manager = DBManager(db_name='solana_insiders_bronze')
        db_manager.insert_raw_dataframe("gmgn_raw_traders", df)
        return {"success": True, "rows": len(df)}
    
    return {"success": False, "reason": "No valid data captured"}

@task(pool='scraping_pool', task_id='scrape_dex3')
def task_scrape_dex3():
    """DEX3 scraper task"""
    _, df = scrape_api_data_sync(
        url='https://dex3.ai/top-wallets?label.is_dev=false&label.is_sniper=false&label.is_insider=false&label.is_top_trader=false&label.is_wash_trader=false&not_label.is_dev=true&not_label.is_sniper=true&not_label.is_insider=false&not_label.is_top_trader=false&not_label.is_wash_trader=true&pnl_7d.pnlMin=0&pnl_30d.pnlMin=0&win_rate_7d.min=50&tokens_30d.max=60&txns_7d.max=100',
        url_pattern=r'api\.dex3\.ai/top-wallets',
        timeout=30
    )
    
    if df is not None:
        # Store DataFrame in database
        from db_manager import DBManager
        db_manager = DBManager(db_name='solana_insiders_bronze')
        db_manager.insert_raw_dataframe("dex3_raw_traders", df)
        return {"success": True, "rows": len(df)}
    
    return {"success": False, "reason": "No valid data captured"}