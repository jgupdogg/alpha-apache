def generate_summary_prompt(article):
    prompt = f"""
    Please summarize the following article.
    Title: {article.title}
    Date: {article.date}
    Content: {article.content}
    """
    return prompt



def generate_keywords_prompt(article):
    prompt = f"""
    Please generate a list of 5 to 10 key words that best represent the most significant concepts, ideas, and entities in the article below, which will eventually be used for sparse vector search. 
    Focus on important terms and the source related to the article's subject matter, and avoid generic words like 'update', 'also', 'important', 'recent', or common stop words.
    The words should help a user retrieve data based on semantic search.  
    
    Title: {article.title}
    Date: {article.date}
    Content: {article.summary}
    
    The keywords should be specific and relevant to the article's core ideas.
    """
    
    return prompt

def get_map_prompt_template(text, summary_type):
    if '***' in text[:5]:
        return f"""
        Below are existing summaries of government releases, separated by '***'.
        After the summary is new, unsummarized text, starting with 'NEW TEXT'.
        Please update the existing summaries with a {summary_type} level summary of the NEW TEXT
        following these guidelines:
        1) Pertains to domestic and global markets only.
        2) Reflects the context of the original document.
        3) Is valuable to an investor.
        4) Adheres to the New York Times' style of writing.
        5) Ends the summary with '***'

        TEXT: 
        {{text}}
        RESPONSE:
        """
    else:
        return f"""
        Below is text from a government document.
        Provide a New York Times style {summary_type} level summary with the following criteria:
        1) Pertains to domestic and global markets only.
        2) Reflects the context of the original document.
        3) Is valuable to an investor.
        4) Provide the summary in structured format with a title and 3-5 bullet points.

        TEXT: 
        {{text}}
        RESPONSE:
        """

def get_combine_prompt_template():
    return """
    Below is a list of excerpts, separated by '***', from a government document.
    Provide a bullet point summary by doing the following:
        1) State a general, actionable title (few words) that the summary is about then list the bullets below it:
        Title: actionable title  
          - Bullet 1
          - Bullet 2 
        2) Do not use more than 10 bullets total. 
        
    EXCERPTS:
    {text}
    RESPONSE:
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
