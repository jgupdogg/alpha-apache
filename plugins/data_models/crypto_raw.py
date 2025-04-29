# utils/data_models/crypto_raw.py

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    JSON,
    Text,
    func,
    Float,
    UniqueConstraint
)


Base = declarative_base()

class TwitterEntities(Base):
    """
    Represents a Twitter account entity snapshot at a particular scrape time.
    E.g. 'account_name', 'joined_date', 'followers', plus a 'scraped_at' timestamp.
    """
    __tablename__ = 'twitter_entities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Time we scraped this row
    scraped_at  = Column(DateTime, default=func.now(), nullable=False, index=True)
    
    account_name = Column(String, nullable=False, index=True)
    joined_date  = Column(String, nullable=True)
    followers    = Column(String, nullable=True)

    # Unique across (account_name, scraped_at); if you prefer also
    # including joined_date, you can add that as well.
    __table_args__ = (
        UniqueConstraint('account_name', 'scraped_at', name='uix_entities_acct_time'),
    )


class TwitterTweets(Base):
    """
    Stores tweets from a given account. The tweet itself already has a 
    'timestamp' (or 'tweet_time'), so we can keep that as-is.
    """
    __tablename__ = 'twitter_tweets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Time of insertion
    timestamp    = Column(DateTime, default=func.now(), nullable=False, index=True)

    account_name = Column(String, nullable=False, index=True)
    tweet_time   = Column(String, nullable=True)  # Time tweet was posted
    tweet_text   = Column(Text, nullable=True)

    num_comments = Column(String, nullable=True)
    num_retweets = Column(String, nullable=True)
    num_likes    = Column(String, nullable=True)
    num_views    = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            'account_name', 'tweet_time', 'tweet_text',
            name='uix_account_name_tweet_time_text'
        ),
    )


class TwitterFollowers(Base):
    """
    Stores followers of a given account. We'll add 'scraped_at' here, too, 
    so you can have multiple snapshots in time of the same followers.
    """
    __tablename__ = 'twitter_followers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Time we scraped this row
    scraped_at       = Column(DateTime, default=func.now(), nullable=False, index=True)

    account_name     = Column(String, nullable=False, index=True)
    follower_account = Column(String, nullable=False, index=True)
    follower_is_verif = Column(String, nullable=True)  # e.g. 'verified' or 'standard'

    # Unique across (account_name, follower_account, scraped_at) 
    # to allow multiple snapshots in time per follower.
    __table_args__ = (
        UniqueConstraint(
            'account_name', 'follower_account', 'scraped_at',
            name='uix_account_follower_time'
        ),
    )
    

# class TopTradersRaw(Base):
#     __tablename__ = 'top_traders_raw'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)
#     blockchain = Column(String, nullable=False, index=True)
#     symbol = Column(String, nullable=False, index=True)
#     data = Column(JSON, nullable=False)


# In utils/data_models/crypto_raw.py


class TokenTwitter(Base):
    __tablename__ = 'token_twitter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)
    blockchain = Column(String, nullable=False, index=True)
    address = Column(String, nullable=False, index=True)
    twitter_url = Column(String, nullable=True)
    scraped_text = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint('blockchain', 'address', name='uix_blockchain_address_tt'),
    )

class TokenTweet(Base):
    """
    Stores detailed tweet data for a given token/project.
    Includes fields for tweet time, text, engagement metrics, and follower count.
    """
    __tablename__ = 'token_tweets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Timestamp of when we scraped/inserted this record
    timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)

    # Basic project identifiers
    blockchain = Column(String, nullable=False, index=True)
    address = Column(String, nullable=False, index=True)

    # The profile URL that generated the tweets
    profile_url = Column(String, nullable=True)

    # Data about the tweet
    tweet_time = Column(String, nullable=True)  # Store as string; convert to DateTime if needed
    tweet_text = Column(Text, nullable=True)    # Text of the tweet itself

    # Engagement metrics
    comments = Column(String, nullable=True)
    retweets = Column(String, nullable=True)
    likes = Column(String, nullable=True)
    views = Column(String, nullable=True)

    # Follower count at time of scraping
    followers = Column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint('blockchain', 'address', 'tweet_time', 'tweet_text', name='uix_blockchain_address_tweet'),
    )




# class BalancesRaw(Base):
#     __tablename__ = 'balances_raw'
    
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)
#     trader_address = Column(String, nullable=False, index=True)
    
#     # Token Attributes
#     blockchain = Column(String, nullable=False, index=True)
#     token_address = Column(String, nullable=False, index=True)
#     name = Column(String, nullable=True)
#     symbol = Column(String, nullable=True)
#     decimals = Column(Integer, nullable=True)
#     balance = Column(String, nullable=True)      # store as string if too large
#     ui_amount = Column(Float, nullable=True)
#     chain_id = Column(String, nullable=True)
#     logo_uri = Column(String, nullable=True)
#     price_usd = Column(Float, nullable=True)
#     value_usd = Column(Float, nullable=True)
    
#     additional_data = Column(String, nullable=True)


class TokenOverviewRaw(Base):
    __tablename__ = 'token_overview_raw'
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=func.now(), nullable=False, index=True)
    blockchain = Column(String, nullable=False, index=True)
    symbol = Column(String, nullable=False, index=True)
    data = Column(JSON, nullable=False)


