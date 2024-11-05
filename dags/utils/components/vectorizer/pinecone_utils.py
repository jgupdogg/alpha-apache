# dags/utils/components/vectorizer/pinecone_utils.py

import os
import logging
from pinecone import Pinecone

logger = logging.getLogger(__name__)

def initialize_pinecone():
    """
    Initializes the Pinecone client and connects to the specified index.
    Assumes the index already exists and does not attempt to create it.

    Returns:
        pinecone.Index: The connected Pinecone index instance.
    """
    # Pinecone connection parameters
    PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
    PINECONE_ENVIRONMENT = os.getenv('PINECONE_ENVIRONMENT')

    if not PINECONE_API_KEY:
        logger.error("Pinecone API key not set in environment variables.")
        return None

    # Initialize Pinecone client
    try:
        logger.info("Initializing Pinecone client...")
        pc = Pinecone(api_key=PINECONE_API_KEY)
        logger.info("Pinecone client initialized successfully.")
    except Exception as e:
        logger.exception(f"Failed to initialize Pinecone client: {e}")
        return None

    # Define the index name
    index_name = 'agent-alpha'  # Replace with your actual Pinecone index name

    # Connect to the existing index
    try:
        logger.info(f"Connecting to Pinecone index '{index_name}'...")
        pinecone_index = pc.Index(index_name)
        logger.info(f"Connected to Pinecone index '{index_name}'.")
        return pinecone_index
    except Exception as e:
        logger.exception(f"Failed to connect to Pinecone index '{index_name}'. Ensure the index exists. Error: {e}")
        return None
