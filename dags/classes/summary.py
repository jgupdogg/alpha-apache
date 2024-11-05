# classes/summary.py

import logging
from datetime import datetime, date
import re
import hashlib
import base64
import json

from pinecone import Index  # Import only the Index class
from utils.components.vectorizer.vectors import create_dense_vector
from utils.components.summaries.summary_func import (
    map_summary, 
    process_text_with_openai
)
from utils.workflows.tickers.prompts import (
    get_initial_summary_prompt,
    get_update_summary_prompt,
    get_sentiment_significance_prompt,
)
from snowflake.snowpark import Session

# Configure detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)

class Summary:
    def __init__(
        self, 
        symbol: str,
        name: str,
        content: str,
        date: datetime = None,
        existing_summary: str = '',
        article_ids: list = None
    ):
        self.symbol = symbol
        self.name = name
        self.content = content
        self.date = date or datetime.now()
        self.existing_summary = existing_summary
        self.summary = None
        self.summary_time = None
        self.dense_vector = None
        self.summary_id = self.generate_summary_id()
        self.article_ids = article_ids or []
        # Initialize attributes to None
        self.headline = None
        self.sentiment_score = None
        self.sentiment_text = None
        self.significance_score = None
        self.significance_text = None

    def generate_summary_id(self):
        """
        Generates a unique summary ID based on the symbol and date.
        """
        date_str = self.date.strftime('%Y%m')
        unique_str = f"{self.symbol}_{date_str}"
        # Hash the unique string
        hash_object = hashlib.sha256(unique_str.encode('utf-8'))
        hex_dig = hash_object.hexdigest()
        summary_id = hex_dig[:12]  # Truncate to 12 characters
        return summary_id

    def retrieve_existing_summary_from_pinecone(self, pinecone_index: Index, namespace: str = 'summaries'):
        """
        Checks if a vector with the same summary_id exists in Pinecone and retrieves it.
        Sets self.existing_summary if found.
        Returns the vector data if it exists, None otherwise.
        """
        vector_id = self.summary_id
        try:
            fetch_response = pinecone_index.fetch(ids=[vector_id], namespace=namespace)
            if fetch_response and fetch_response.get('vectors') and vector_id in fetch_response['vectors']:
                vector_data = fetch_response['vectors'][vector_id]
                logger.info(f"Retrieved existing vector from Pinecone with id {vector_id}")
                # Set existing_summary from metadata
                self.existing_summary = vector_data['metadata'].get('summary', '')
                return vector_data
            else:
                logger.info(f"No existing vector found in Pinecone with id {vector_id}")
                return None
        except Exception as e:
            logger.exception(f"Error retrieving existing vector from Pinecone: {str(e)}")
            return None

    def gen_map_summary(self):
        """
        Generates and stores the summary of the content.
        """
        raw_summary = map_summary(self)
        # Split the title and summary
        parts = raw_summary.split('\n', 1)
        if len(parts) > 1:
            self.summary = parts[1].strip()
        else:
            self.summary = raw_summary
        return self.summary

    def generate_summary(self):
        """
        Generates and stores the summary of the content.
        """
        if not self.existing_summary:
            logger.info('Generating new summary')
            prompt = get_initial_summary_prompt(self.symbol, self.name)
        else:
            logger.info('Updating existing summary')
            prompt = get_update_summary_prompt(self.symbol, self.name, self.existing_summary)

        # Prepare the content for the prompt
        prompt += f"\n{self.content}"

        # Processing the first prompt
        summarized_text = process_text_with_openai(self.content, prompt)
        if not summarized_text or len(summarized_text) < 100:
            logger.warning(f"Summary too short for summary ID {self.summary_id}")
            return None

        # Second prompt for adding Sentiment and Significance
        prompt2 = get_sentiment_significance_prompt(self.symbol, self.name)
        sentiment_significance = process_text_with_openai(summarized_text, prompt2)

        # Combining the results from both prompts
        final_output = "Summary: " + summarized_text + '\n' + sentiment_significance

        self.summary = final_output
        self.summary_time = datetime.now()
        return self.summary

    def parse_sentiment_significance_output(self, output_str):
        """
        Parses the output from the sentiment and significance prompt and stores the results.
        """
        # Initialize variables
        self.headline = None
        self.sentiment_score = None
        self.sentiment_text = ''
        self.significance_score = None
        self.significance_text = ''

        # Split the output into lines
        lines = output_str.strip().split('\n')

        # Initialize variables to hold the parts
        current_section = None

        for line in lines:
            line = line.strip()
            if line.lower().startswith('headline:'):
                self.headline = line[len('headline:'):].strip()
                current_section = None
            elif line.lower().startswith('sentiment:'):
                sentiment_line = line[len('sentiment:'):].strip()
                sentiment_parts = sentiment_line.split('-', 1)
                if len(sentiment_parts) == 2:
                    score_part, text_part = sentiment_parts
                    score_part = score_part.strip()
                    text_part = text_part.strip()
                    # Extract score
                    if '/' in score_part:
                        score_str, _ = score_part.split('/', 1)
                        try:
                            self.sentiment_score = float(score_str.strip())
                        except ValueError:
                            self.sentiment_score = None
                    else:
                        self.sentiment_score = None
                    self.sentiment_text = text_part
                else:
                    self.sentiment_text = sentiment_line
                current_section = 'sentiment'
            elif line.lower().startswith('significance:'):
                significance_line = line[len('significance:'):].strip()
                significance_parts = significance_line.split('-', 1)
                if len(significance_parts) == 2:
                    score_part, text_part = significance_parts
                    score_part = score_part.strip()
                    text_part = text_part.strip()
                    # Extract score
                    if '/' in score_part:
                        score_str, _ = score_part.split('/', 1)
                        try:
                            self.significance_score = float(score_str.strip())
                        except ValueError:
                            self.significance_score = None
                    else:
                        self.significance_score = None
                    self.significance_text = text_part
                else:
                    self.significance_text = significance_line
                current_section = 'significance'
            else:
                # Continuation of previous section
                if current_section == 'sentiment':
                    self.sentiment_text += ' ' + line
                elif current_section == 'significance':
                    self.significance_text += ' ' + line

    def generate_sentiment_significance(self):
        """
        Generates and stores the sentiment and significance analysis of the summary.
        """
        # Ensure we have a summary
        if not self.summary:
            logger.warning(f"Summary not found for summary ID {self.summary_id}, generating summary first.")
            self.generate_summary()
            if not self.summary:
                logger.error(f"Failed to generate summary for summary ID {self.summary_id}, cannot generate sentiment and significance.")
                return None

        # Prepare the prompt
        prompt = get_sentiment_significance_prompt(self.symbol, self.name)

        # Process the text with OpenAI
        sentiment_significance_output = process_text_with_openai(self.summary, prompt)

        if not sentiment_significance_output:
            logger.error(f"Failed to generate sentiment and significance for summary ID {self.summary_id}.")
            return None

        # Parse the output
        self.parse_sentiment_significance_output(sentiment_significance_output)
        logger.info(f"Generated sentiment and significance for summary ID {self.summary_id}")

    def generate_dense_vector(self):
        """
        Generates and stores the dense vector representation of the summary.
        """
        self.dense_vector = create_dense_vector(self.summary)
        return self.dense_vector

    def generate_all_representations(self):
        """
        Generates summary, sentiment/significance, and dense vector in the correct order.
        """
        self.gen_map_summary()
        logger.info(f"Generating summary for summary ID {self.summary_id}")
        self.generate_summary()
        logger.info(f"Generated summary for summary ID {self.summary_id}")
        self.generate_sentiment_significance()
        logger.info(f"Generated sentiment and significance for summary ID {self.summary_id}")
        self.generate_dense_vector()
        logger.info(f"Generated dense vector for summary ID {self.summary_id}")
        return {
            'summary': self.summary,
            'headline': self.headline,
            'sentiment_score': self.sentiment_score,
            'sentiment_text': self.sentiment_text,
            'significance_score': self.significance_score,
            'significance_text': self.significance_text,
            'dense_vector': self.dense_vector,
        }

    def upsert_to_pinecone(self, pinecone_index: Index, namespace: str = 'summaries'):
        logger.info(f"Upserting summary {self.summary_id} to Pinecone...")

        # Prepare the metadata
        metadata = {
            'date': self.date.isoformat(),
            'summary': self.summary,
            'symbol': self.symbol,
            'headline': self.headline,
            'sentiment_score': self.sentiment_score,
            'sentiment_text': self.sentiment_text,
            'significance_score': self.significance_score,
            'significance_text': self.significance_text,
            'article_ids': ",".join(self.article_ids)  # Serialize list to comma-separated string
            # Include other metadata fields as needed
        }

        # Remove any metadata entries where the value is None
        metadata = {k: v for k, v in metadata.items() if v is not None}

        # Prepare the vector for upsert
        vector = {
            'id': self.summary_id,
            'values': self.dense_vector,
            'metadata': metadata
        }

        # Upsert the vector to Pinecone
        try:
            upsert_response = pinecone_index.upsert(
                vectors=[vector],
                namespace=namespace
            )
            logger.info(f"Successfully upserted summary {self.summary_id} to Pinecone")
            return upsert_response
        except Exception as e:
            logger.exception(f"Error upserting summary {self.summary_id} to Pinecone: {str(e)}")
            return None

    def to_dict(self):
        """
        Converts the summary object to a dictionary.
        """
        return {
            'SUMMARY_ID': self.summary_id,
            'SYMBOL': self.symbol,
            'SUMMARY_TIME': self.summary_time.strftime('%Y-%m-%d %H:%M:%S') if self.summary_time else None,
            # Include other fields as necessary
        }

    def update_articles_in_snowflake(self, snowflake_session: Session):
        """
        Updates the SUMMARY_ID and SUMMARY_TIME fields in the STOCK_NEWS table for associated articles.
        """
        if not self.article_ids:
            logger.warning(f"No article IDs associated with summary {self.summary_id}")
            return

        try:
            article_ids_db = self.article_ids
            ids_str = ",".join(f"'{id_}'" for id_ in article_ids_db)
            summary_time_str = self.summary_time.strftime('%Y-%m-%d %H:%M:%S') if self.summary_time else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            update_summary_query = f"""
            UPDATE STOCK_NEWS
            SET SUMMARY_ID = '{self.summary_id}', SUMMARY_TIME = '{summary_time_str}'
            WHERE ID IN ({ids_str});
            """

            logger.debug(f"Update SUMMARY_ID and SUMMARY_TIME query: {update_summary_query}")

            logger.info(f"Executing update SUMMARY_ID and SUMMARY_TIME for summary {self.summary_id}.")
            snowflake_session.sql(update_summary_query).collect()
            logger.info(f"Marked articles as summarized for summary {self.summary_id}.")
        except Exception as e:
            logger.exception(f"Failed to update SUMMARY_ID and SUMMARY_TIME for summary {self.summary_id}: {e}")

