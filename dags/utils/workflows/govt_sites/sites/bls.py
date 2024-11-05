from bs4 import Comment
from datetime import datetime
import re

def parse_bls_news_releases(soup):
    news_releases = []
    
    # Find all comments in the soup
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    
    # Find the comment that marks the start of the news release list
    start_comment = None
    for comment in comments:
        if 'begin latest news release list' in comment.lower():
            start_comment = comment
            break
    
    if start_comment:
        # Find the ul element that follows this comment
        ul_element = start_comment.find_next('ul')
        
        if ul_element:
            # Find all list items within the unordered list
            list_items = ul_element.find_all('li')
            
            for item in list_items:
                link = item.find('a')
                if link:
                    title = link.text.strip()
                    href = link.get('href')
                    
                    # Remove '.toc' from the href
                    href = href.replace('.toc', '')
                    
                    # Extract the date using regex
                    date_match = re.search(r'\d{2}/\d{2}/\d{4}', item.text)
                    if date_match:
                        date_str = date_match.group()
                        try:
                            date = datetime.strptime(date_str, '%m/%d/%Y').date()
                        except ValueError:
                            date = date_str  # If parsing fails, store the original string
                    else:
                        date = None  # If no date is found
                    
                    news_releases.append({
                        'title': title,
                        'link': f"https://www.bls.gov{href}" if href.startswith('/') else href,
                        'date': date
                    })
    
    return news_releases

def parse_bls_article_content(soup):
    article_content = {}
    
    # Extract the title
    h1_element = soup.find('h1')
    if h1_element:
        article_content['title'] = h1_element.text.strip()
    
    # Find the main content div
    main_content = soup.find('div', class_='main-content')
    if main_content:
        # Find the pre tag within the normalnews div
        pre_content = main_content.find('div', class_='normalnews').find('pre')
        if pre_content:
            # Extract and clean the text content
            content = pre_content.get_text(strip=True)
            
            # Remove extra whitespace and newlines
            content = re.sub(r'\n\s*\n', '\n\n', content)
            content = re.sub(r' +', ' ', content)
            
            article_content['content'] = content.strip()
            
            # Extract the release date and time
            date_match = re.search(r'For release (\d{1,2}:\d{2} [ap]\.m\. \(ET\) \w+, \w+ \d{1,2}, \d{4})', content)
            if date_match:
                article_content['release_date'] = date_match.group(1)
            
            # Extract the USDL number
            usdl_match = re.search(r'USDL-(\d{2}-\d{4})', content)
            if usdl_match:
                article_content['usdl_number'] = usdl_match.group(1)
    
    return article_content
