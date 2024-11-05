# dags/utils/workflows/tickers/prompts.py

def get_initial_summary_prompt(symbol, name):
    return f"""
    Below are excerpts separated by '*** SOURCE: X' where X is the source ID.
    Mentally consider the most recent and talked about topics related to {name} ({symbol}) from the excerpts. 
    Provide a concise, mid-level bullet point summary about these topics as they relate to {name} in this format:
      - Bullet 1 (source X).
      - Bullet 2 (source X).
      - etc...    
    Do not use more than 5 bullets and use New York Times style. 
    Do not extract financial advice, opinions, general, non-important, old information. 
    Make sure to include the source ID after each bullet.
    All bullets must be related to {name}.
    """

def get_update_summary_prompt(symbol, name, existing_summary):
    return f"""
    This is the current bullet summary of excerpts about {name} ({symbol}), with citations denoted as 
    (source X), where X is the source ID:
    {existing_summary}
    
    Below are new excerpts separated by '*** SOURCE: X' where X is the source ID.
    Our job is to evaluate the new excerpts and update the bullets above only if there is new, significant, high level information about {name}
    that is not present in the current summary.  
    Updates are not required for financial advice, opinions, general or non-important information. 
    If no update is needed, respond with:
        "no new info."
    If we must update the summary:
        1) Use the Source ID to provide citations after each modified or new bullet in this format:
          - Bullet (source X).
        2) Maintain existing citations if that information remains in the bullet.
        3) Do not use more than 5 bullets total.  
    """

def get_sentiment_significance_prompt(symbol, name):
    return f"""
    You are a financial reporter. Based on the text provided, create the following:
    1) A very brief New York Times style headline that reflects the most significant recent events affecting {name} ({symbol}).
    2) A sentiment score from 0 (bad) to 5 (good), with an explanation for the score.
    3) A significance score from 0 (low) to 5 (high), with an explanation for the score. Consider the context of price action below and include a comment on how 
    the market has reacted based on price action.  
    
    Follow this format for the output:
    Headline: Headline
    Sentiment: X/5 - A few sentences.
    Significance: X/5 - A few sentences.
    """
