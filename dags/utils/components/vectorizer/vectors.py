import openai
from pinecone import Pinecone
import os
from datetime import date
from dotenv import load_dotenv
# from transformers import AutoTokenizer, AutoModelForMaskedLM
# import torch

# Load environment variables
load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')
pinecone_api_key = os.getenv('PINECONE_API_KEY')
pinecone_index_name = os.getenv('PINECONE_INDEX_NAME')


# Initialize Pinecone client
pc = Pinecone(api_key=f'{pinecone_api_key}')
index = pc.Index(pinecone_index_name)

# Initialize BERT model and tokenizer for SPLADE-like functionality
# model_id = 'naver/splade-cocondenser-ensembledistil'
# tokenizer = AutoTokenizer.from_pretrained(model_id)
# model = AutoModelForMaskedLM.from_pretrained(model_id)
# model.eval()



# class SPLADELikeVectorizer:
#     def __init__(self, max_dimension=1536):
#         self.model = model
#         self.tokenizer = tokenizer
#         self.max_dimension = max_dimension

#     def generate_sparse_vector(self, text):
#         tokens = self.tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=512)
#         with torch.no_grad():
#             output = self.model(**tokens)
        
#         vec = torch.max(
#             torch.log(1 + torch.relu(output.logits)) * tokens.attention_mask.unsqueeze(-1),
#             dim=1
#         )[0].squeeze()
        
#         cols = vec.nonzero().squeeze().cpu().tolist()
#         weights = vec[cols].cpu().tolist()
        
#         # Normalize weights
#         if weights:
#             max_weight = max(weights)
#             weights = [w / max_weight for w in weights]
        
#         return {
#             'indices': cols,
#             'values': weights
#         }


# def create_sparse_vector(words):
#     vectorizer = SPLADELikeVectorizer()
#     if isinstance(words, list):
#         words = " ".join(words)
#     sparse_vector = vectorizer.generate_sparse_vector(words)
#     return sparse_vector



def create_dense_vector(input_text, model="text-embedding-3-small"):
    """
    Creates an embedding for the given input text using OpenAI's embedding model.

    Parameters:
    input_text (str or list): The text (or list of texts) to embed.
    model (str): The model to use for creating embeddings (default is 'text-embedding-ada-002').

    Returns:
    dict: The embedding object containing the embedding vector and additional information.
    """
    
    try:
        # Send the embedding request to the API
        response = openai.Embedding.create(
            input=input_text,
            model=model
        )
        
        # Extract the embedding vector from the response
        embedding = response['data'][0]['embedding']
        return embedding

    except Exception as e:
        print(f"Error creating dense vector: {str(e)}")
        return None
    

def upsert_to_pinecone(article):
    """
    Format and upsert article data to Pinecone.
    
    :param article: Article object containing all necessary data
    """
    if not all([article.summary, article.keywords, article.dense_vector, article.sparse_vector]):
        article.generate_all_representations()

    vector = {
        'id': article.id,
        'values': article.dense_vector,
        'sparse_values': article.sparse_vector,
        'metadata': {
            'title': article.title,
            'ai_generated_title': article.ai_generated_title,
            'date': article.date.isoformat() if isinstance(article.date, date) else article.date,
            'keywords': article.keywords,
            'summary': article.summary,
            'symbol': article.symbol
        }
    }

    # Upsert the vector to Pinecone
    try:
        upsert_response = index.upsert(vectors=[vector], namespace='articles')
        print(f"Successfully upserted article {article.id} to Pinecone")
        return upsert_response
    except Exception as e:
        print(f"Error upserting article {article.id} to Pinecone: {str(e)}")
        return None




