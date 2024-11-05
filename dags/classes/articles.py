# dags/utils/components/articles.py

import json
from datetime import datetime
import logging
import re
import hashlib
import base64

# Configure detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class Article:
    def __init__(
        self, 
        title: str, 
        content: str, 
        date_str: str, 
        symbol: str, 
        category: str = 'news', 
        **kwargs
    ):
        self.title = title
        self.content = content
        self.date = self._parse_date(date_str)
        self.symbol = symbol
        self.category = category
        self.additional_info = kwargs
        self.url = self.additional_info.get('url', None)
        self.site = self.additional_info.get('site', None)
        self.id = self._generate_id()

    def _parse_date(self, date_str):
        """
        Parses the date string into a datetime object.
        """
        if not isinstance(date_str, str):
            logger.warning(f"Invalid date_str: {date_str} (type: {type(date_str)})")
            return date_str
        date_formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        logger.warning(f"Unable to parse date: {date_str}")
        return date_str


    def _generate_id(self, length=12):
            """
            Generates a unique, fixed-length ID for the article using a hash function.

            Parameters:
            - length (int): The desired length of the ID.

            Returns:
            - str: A unique ID string.
            """
            identifier = self.url if self.url else self.title
            if not identifier:
                raise ValueError("Cannot generate ID without a URL or title.")
            # Clean the identifier to remove non-alphanumeric characters
            identifier = re.sub(r'\W+', '', identifier)
            # Combine symbol, date, and identifier
            unique_str = f"{self.symbol}_{self.date.isoformat()}_{identifier}"
            # Generate a SHA-256 hash of the unique string
            hash_object = hashlib.sha256(unique_str.encode('utf-8'))
            # Convert the hash to a hexadecimal string
            hex_dig = hash_object.hexdigest()
            # Truncate the hash to the desired length
            id_candidate = hex_dig[:length]
            return id_candidate

    def to_dict(self):
        """
        Converts the article object to a dictionary.
        """
        return {
            'ID': self.id,
            'SYMBOL': self.symbol,
            'TITLE': self.title,
            'PUBLISHEDDATE': self.date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(self.date, datetime) else self.date,
            'SITE': self.site,
            'TEXT': self.content,
            'URL': self.url,
            'CATEGORY': self.category,
            # Include other fields as necessary
        }

    def to_json(self):
        """
        Converts the article object to a JSON string.
        """
        return json.dumps(self.to_dict(), indent=2, cls=DateEncoder)

    def upsert_to_snowflake(self, snowflake_session):
        """
        Performs an upsert operation into the stock_news table in Snowflake.
        """
        data = self.to_dict()
        symbol = data.get('SYMBOL')
        published_date = data.get('PUBLISHEDDATE')
        category = data.get('CATEGORY')
        title = data.get('TITLE')

        if not symbol or not published_date or not title:
            logger.error(f"Missing required fields in article. Skipping upsert.")
            return

        # Prepare columns and values
        columns = list(data.keys())
        values = []
        for col in columns:
            val = data[col]
            if val is None:
                values.append('NULL')
            elif isinstance(val, datetime):
                values.append(f"TO_TIMESTAMP('{val.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')")
            else:
                sanitized_value = str(val).replace("'", "''")  # Escape single quotes
                values.append(f"'{sanitized_value}'")

        # Construct the SET clause for matched rows (excluding the key columns)
        key_columns = ['SYMBOL', 'PUBLISHEDDATE', 'CATEGORY', 'TITLE']
        set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns if col not in key_columns])

        # Construct the MERGE statement
        merge_query = f"""
        MERGE INTO stock_news AS target
        USING (SELECT {', '.join([f"{v} AS {k}" for k, v in zip(columns, values)])}) AS source
        ON target.SYMBOL = source.SYMBOL 
           AND target.PUBLISHEDDATE = source.PUBLISHEDDATE 
           AND target.CATEGORY = source.CATEGORY 
           AND target.TITLE = source.TITLE
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(columns)})
            VALUES ({', '.join(['source.' + col for col in columns])});
        """

        try:
            snowflake_session.sql(merge_query).collect()
            logger.info(f"Upserted article into stock_news for symbol {symbol}, category {category}, date {published_date}.")
        except Exception as e:
            logger.exception(f"Failed to upsert article into stock_news for symbol {symbol}, category {category}, date {published_date}: {e}")

    @classmethod
    def from_dict(cls, data):
        # Normalize keys to lowercase
        data = {k.lower(): v for k, v in data.items()}
        title = data.pop('title', '')
        content = data.pop('content', '')
        date_str = data.pop('date', '')
        symbol = data.pop('symbol', '')
        category = data.pop('category', 'news')
        return cls(title, content, date_str, symbol, category=category, **data)

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls.from_dict(data)
