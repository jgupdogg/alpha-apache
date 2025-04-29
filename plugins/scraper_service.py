from flask import Flask, request, jsonify
import asyncio
import nest_asyncio
import logging
import json
from datetime import datetime
from utils.nodriver_scraper import WebScraper
from utils.token_class import Token
from db_manager import DBManager

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Global variables for scrapers
scrapers = {}
initialized = False

# Apply nest_asyncio for running async code
nest_asyncio.apply()
loop = asyncio.get_event_loop()

# Initialize scrapers on startup
@app.before_first_request
def initialize_scrapers():
    global scrapers, initialized
    
    if initialized:
        return
    
    profile_dir = "/home/jgupdogg/airflow/plugins/profiles"
    
    async def init():
        try:
            # Initialize DexScreener scraper
            logger.info("Initializing DexScreener scraper")
            dex_scraper = WebScraper(
                headless=False, 
                user_data_dir=profile_dir,
                profile_name='dexscreener_auth',
                use_virtual_display=True,
            )
            await dex_scraper.start()
            await dex_scraper.load_cookies()
            scrapers['dexscreener'] = dex_scraper
            
            # Initialize X/Twitter scraper
            logger.info("Initializing X/Twitter scraper")
            x_scraper = WebScraper(
                headless=False,
                user_data_dir=profile_dir,
                profile_name='x_auth'
            )
            await x_scraper.start()
            await x_scraper.load_cookies()
            scrapers['x'] = x_scraper
            
            # Initialize SolSniffer scraper
            logger.info("Initializing SolSniffer scraper")
            sol_scraper = WebScraper(headless=True)
            await sol_scraper.start()
            scrapers['solsniffer'] = sol_scraper
            
            initialized = True
            logger.info("All scrapers initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing scrapers: {str(e)}")
            # Close any scrapers that were successfully initialized
            for name, scraper in scrapers.items():
                try:
                    await scraper.stop()
                except:
                    pass
            scrapers = {}
    
    loop.run_until_complete(init())

@app.route('/health', methods=['GET'])
def health():
    global scrapers, initialized
    return jsonify({
        'status': 'healthy' if initialized else 'initializing',
        'scrapers_initialized': list(scrapers.keys()),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/scrape/token', methods=['POST'])
def scrape_token():
    global scrapers
    
    if not initialized:
        return jsonify({'error': 'Scrapers not initialized yet'}), 503
    
    # Get token addresses from request
    data = request.json
    if not data or 'tokens' not in data:
        return jsonify({'error': 'Missing token addresses'}), 400
    
    token_list = data['tokens']
    logger.info(f"Received request to scrape {len(token_list)} tokens")
    
    # Filter out SOL and stablecoins
    token_list = [t for t in token_list if t not in [
        'So11111111111111111111111111111111111111112', 
        'he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A', 
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    ]]
    
    if not token_list:
        return jsonify({'message': 'No tokens to process after filtering'}), 200
    
    # Create database manager
    db = DBManager(db_name="solana")
    results = {}
    
    try:
        for token_address in token_list:
            if not token_address:
                continue
                
            try:
                logger.info(f"Processing token {token_address}")
                
                # Create token object with scrapers
                token = Token(token_address, db_manager=db, scrapers=scrapers)
                token_data = {}
                
                # Get metadata
                metadata = token.get_token_metadata()
                if metadata:
                    token_data['metadata'] = metadata
                
                # Get market data
                market_data = token.get_token_market_data()
                if market_data:
                    token_data['market_data'] = market_data
                
                # Get Twitter/X handle
                x_handle = token.get_x_handle()
                if x_handle:
                    token_data['x_handle'] = x_handle
                    
                    # Get Twitter/X followers if handle exists
                    x_followers = token.get_x_followers()
                    if x_followers:
                        token_data['x_followers'] = x_followers
                
                # Get SolSniffer score
                sniff_score = token.get_sniff_score()
                if sniff_score is not None:
                    token_data['sniff_score'] = sniff_score
                
                # Get token creation info
                creation_info = token.get_token_creation_info()
                if creation_info:
                    token_data['creation_info'] = creation_info
                
                # Store results
                results[token_address] = {
                    'success': True,
                    'data': token_data
                }
                logger.info(f"Successfully processed token {token_address}")
                
            except Exception as e:
                logger.error(f"Error processing token {token_address}: {str(e)}")
                results[token_address] = {
                    'success': False,
                    'error': str(e)
                }
                
    finally:
        # Close the database connection
        db.quit()
    
    return jsonify({
        'processed_tokens': len(results),
        'results': results
    })

@app.route('/reset', methods=['POST'])
def reset_scrapers():
    global scrapers, initialized
    
    async def reset():
        # Close all scrapers
        for name, scraper in scrapers.items():
            try:
                logger.info(f"Closing {name} scraper")
                await scraper.stop()
            except Exception as e:
                logger.error(f"Error closing {name} scraper: {str(e)}")
        
        # Clear scrapers dict
        scrapers = {}
        initialized = False
        
        # Re-initialize
        initialize_scrapers()
    
    loop.run_until_complete(reset())
    return jsonify({'status': 'Scrapers reset successfully'})

if __name__ == '__main__':
    # Initialize scrapers before starting the app
    initialize_scrapers()
    app.run(host='0.0.0.0', port=5000)