import openai
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from langchain.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain
from langchain.chat_models import ChatOpenAI
import time
import os
from dotenv import load_dotenv
from dags.utils.workflows.govt_sites.prompts import *
import logging

logging.basicConfig(level=logging.INFO)


load_dotenv()
# Get the OpenAI API key
api_key = os.getenv('OPENAI_API_KEY')

# Set up your OpenAI API key
openai.api_key = api_key

model = 'gpt-3.5-turbo'

def call_openai(prompt, model=model, max_retries=10, timeout_duration=15):
    
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": prompt}
    ]
    
    start_time = time.time()
    attempt = 0
    
    while attempt < max_retries:
        try:
            response = openai.ChatCompletion.create(
                model=model,
                messages=messages,
                timeout=timeout_duration
            )
            return response.choices[0].message.content.strip()
        
        except (openai.error.RateLimitError, openai.error.APIError, openai.error.ServiceUnavailableError) as e:
            logging.error(f"Error occurred: {str(e)}, retrying...")
            attempt += 1
            time.sleep(2 ** attempt)  # exponential backoff
        
        except openai.error.TimeoutError as e:
            elapsed_time = time.time() - start_time
            if elapsed_time < timeout_duration * max_retries:
                logging.error(f"Request timed out after {timeout_duration} seconds, sending a new request...")
                attempt += 1
                continue
            else:
                raise e  # re-raise the exception if we're out of time
        
        except Exception as e:
            logging.error(f"Unexpected error occurred: {str(e)}")
            raise e
    
    raise Exception("Failed to get a response within the time limit after multiple retries.")


def process_text_with_openai(text, prompt):
    """
    Processes the given text with the provided prompt using OpenAI's API.
    """
    openai.api_key = api_key

    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": text}
            ],
            max_tokens=500,
            temperature=0.7,
        )
        generated_text = response['choices'][0]['message']['content'].strip()
        return generated_text
    except Exception as e:
        logging.exception(f"OpenAI API call failed: {e}")
        return None

def map_summary(article, summary_type='deep', model=model, chunk_size=5000, chunk_overlap=200, verbose=False):

    """
    Performs a map-reduce summarization on the given text with a structured output format.

    Args:
    article (Article): The article object to summarize.
    summary_type (str): The type of summary to generate ('deep', 'mid', or 'shallow').
    model (str): The OpenAI model to use for summarization.
    chunk_size (int): The size of text chunks for splitting.
    chunk_overlap (int): The overlap between text chunks.
    verbose (bool): Whether to print verbose output.

    Returns:
    dict: The structured summary with a title and bullet points.
    """

    # Split the text into chunks
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    docs = [Document(page_content=t) for t in text_splitter.split_text(article.content)]  # Access content with dot notation
    logging.info(f"Split the text into {len(docs)} documents.")

    # Adjust the summary type based on the number of documents
    if summary_type == 'deep' and len(docs) > 20:
        summary_type = 'mid'
        docs = docs[:6] + docs[6:-5:2] + docs[-5:]
    if summary_type == 'mid' and len(docs) >= 40:
        docs = docs[:10] + docs[10:-5:3] + docs[-5:]

    logging.info(f"content peak: {article.content[:100]}")
    
    # Define prompt templates
    map_prompt = PromptTemplate(template=get_map_prompt_template(article.content, summary_type), input_variables=["text"])
    combine_prompt = PromptTemplate(template=get_combine_prompt_template(), input_variables=["text"])


    summary_chain = load_summarize_chain(
        ChatOpenAI(temperature=0, model_name=model, request_timeout=1000, openai_api_key=api_key),
        chain_type="map_reduce",
        map_prompt=map_prompt,
        combine_prompt=combine_prompt,
        verbose=verbose,
    )

    # Running the summarization process and returning results``
    logging.info(f"Docs: {docs}")

    summary = summary_chain.run(docs)

    logging.info(f"Type of summary: {type(summary)}")
    logging.debug(f"Summary content: {summary}")

    return summary
