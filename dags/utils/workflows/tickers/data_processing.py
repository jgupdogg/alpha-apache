# dags/utils/data_processing.py

from typing import Dict, List
from dags.utils.workflows.tickers.data_models import (
    Profile, Metrics, Ratios, InsideTrade, SplitHistory, StockDividend, StockNews
)
import datetime
import hashlib


def process_company_data(data: Dict, symbol: str) -> Dict:
    # Extract data for each table
    profile_data = data.get('profile', {})
    metrics_data = data.get('metrics', {})
    ratios_data = data.get('ratios', [{}])
    inside_trades_data = data.get('insideTrades', [])
    splits_history_data = data.get('splitsHistory', [])
    stock_dividends_data = data.get('stockDividend', [])
    stock_news_data = data.get('stockNews', [])

    return {
        'symbol': symbol,
        'profile': profile_data,
        'metrics': metrics_data,
        'ratios': ratios_data,
        'inside_trades': inside_trades_data,
        'splits_history': splits_history_data,
        'stock_dividends': stock_dividends_data,
        'stock_news': stock_news_data
    }

def normalize_profile_data(profile: Dict) -> Dict:
    profile_normalized = {}
    profile_fields = Profile.__annotations__.keys()
    for field in profile_fields:
        value = profile.get(field)
        if value is None:
            profile_normalized[field] = None
        else:
            # Convert value to correct type
            if field in ['price', 'beta', 'lastDiv', 'dcfDiff', 'dcf', 'changes']:
                profile_normalized[field] = float(value)
            elif field in ['volAvg', 'mktCap']:
                profile_normalized[field] = int(value)
            elif field in ['defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund']:
                profile_normalized[field] = bool(value)
            elif field == 'ipoDate':
                profile_normalized[field] = parse_date(value)
            else:
                profile_normalized[field] = str(value)
    return profile_normalized

def normalize_metrics_data(metrics: Dict) -> Dict:
    metrics_normalized = {}
    metrics_fields = Metrics.__annotations__.keys()
    for field in metrics_fields:
        value = metrics.get(field)
        if value is None:
            metrics_normalized[field] = None
        else:
            if field in ['dividendYielTTM', 'yearHigh', 'yearLow']:
                metrics_normalized[field] = float(value)
            elif field == 'volume':
                metrics_normalized[field] = int(value)
    return metrics_normalized

def normalize_ratios_data(ratios: Dict) -> Dict:
    ratios_normalized = {}
    ratios_fields = Ratios.__annotations__.keys()
    for field in ratios_fields:
        value = ratios.get(field)
        if value is None:
            ratios_normalized[field] = None
        else:
            ratios_normalized[field] = float(value)
    return ratios_normalized

def normalize_inside_trade(trade: Dict) -> Dict:
    trade_normalized = {}
    trade_fields = InsideTrade.__annotations__.keys()
    for field in trade_fields:
        value = trade.get(field)
        if value is None:
            trade_normalized[field] = None
        else:
            if field in ['securitiesOwned', 'securitiesTransacted']:
                trade_normalized[field] = int(value)
            elif field == 'price':
                trade_normalized[field] = float(value)
            elif field in ['filingDate', 'transactionDate']:
                trade_normalized[field] = parse_datetime(value)
            else:
                trade_normalized[field] = str(value)
    return trade_normalized

def normalize_splits_history(split: Dict) -> Dict:
    split_normalized = {}
    split_fields = SplitHistory.__annotations__.keys()
    for field in split_fields:
        value = split.get(field)
        if value is None:
            split_normalized[field] = None
        else:
            if field in ['numerator', 'denominator']:
                split_normalized[field] = int(value)
            elif field == 'date':
                split_normalized[field] = parse_date(value)
            else:
                split_normalized[field] = str(value)
    return split_normalized

def normalize_stock_dividend(dividend: Dict) -> Dict:
    dividend_normalized = {}
    dividend_fields = StockDividend.__annotations__.keys()
    for field in dividend_fields:
        value = dividend.get(field)
        if value is None:
            dividend_normalized[field] = None
        else:
            if field in ['adjDividend', 'dividend']:
                dividend_normalized[field] = float(value)
            elif field in ['date', 'recordDate', 'paymentDate', 'declarationDate']:
                dividend_normalized[field] = parse_date(value)
            else:
                dividend_normalized[field] = str(value)
    return dividend_normalized

def normalize_stock_news(news: Dict) -> Dict:
    news_normalized = {}
    news_fields = StockNews.__annotations__.keys()
    for field in news_fields:
        value = news.get(field)
        if value is None:
            news_normalized[field] = None
        else:
            if field == 'publishedDate':
                news_normalized[field] = parse_datetime(value)
            else:
                news_normalized[field] = str(value)
    # Generate ID
    news_normalized['ID'] = generate_id(
        news_normalized['symbol'],
        news_normalized.get('category', 'news'),
        news_normalized['publishedDate'].isoformat() if news_normalized['publishedDate'] else '',
        news_normalized['title']
    )
    # Set SUMMARY_TIME
    news_normalized['SUMMARY_TIME'] = news_normalized['publishedDate']
    # 'SUMMARY_ID' can be set to None or left as is
    news_normalized['SUMMARY_ID'] = None
    return news_normalized

def normalize_press_release(press: Dict) -> Dict:
    press_normalized = {}
    # Map fields to match stock_news table
    press_normalized['symbol'] = press.get('symbol')
    press_normalized['title'] = press.get('title')
    press_normalized['text'] = press.get('text')
    press_normalized['category'] = 'press'
    date_str = press.get('date')
    if date_str:
        press_normalized['publishedDate'] = parse_datetime(date_str)
        press_normalized['SUMMARY_TIME'] = press_normalized['publishedDate']
    else:
        press_normalized['publishedDate'] = None
        press_normalized['SUMMARY_TIME'] = None
    # The press release data may not have 'image', 'site', 'url'
    press_normalized['image'] = None
    press_normalized['site'] = None
    press_normalized['url'] = None
    # Generate ID
    press_normalized['ID'] = generate_id(
        press_normalized['symbol'],
        'press',
        date_str if date_str else '',
        press_normalized['title']
    )
    press_normalized['SUMMARY_ID'] = None
    return press_normalized


def parse_date(value: str):
    # Parse date string to date object
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return None

def parse_datetime(value: str):
    # Parse datetime string to datetime object
    date_formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d'
    ]
    for fmt in date_formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

    

def generate_id(symbol: str, category: str, published_date: str, title: str) -> str:
    # Create a unique ID using hash
    unique_str = f"{symbol}_{category}_{published_date}_{title}"
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

