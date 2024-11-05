
from datetime import datetime

def parse_ny_fed_news_links(soup):
    news_links = []
    
    # Find all news link rows
    news_rows = soup.find_all('tr', class_='About')
    
    for row in news_rows:
        news_item = {}
        
        # Extract date
        date_div = row.find('td', class_='dirColL').find('div')
        if date_div:
            try:
                news_item['date'] = datetime.strptime(date_div.text.strip(), '%b %d, %Y').date()
            except ValueError:
                news_item['date'] = date_div.text.strip()
        
        # Extract title and link
        link_elem = row.find('a', class_='paraHeader')
        if link_elem:
            news_item['title'] = link_elem.text.strip()
            news_item['link'] = f"https://www.newyorkfed.org{link_elem['href']}"
        
        if news_item:
            news_links.append(news_item)
    
    return news_links



def parse_ny_fed_article_content(soup):
    article_content = {
        'title': '',
        'content': '',
        'date': None
    }
    
    # Extract title
    title_elem = soup.find('div', class_='ts-article-title')
    if title_elem:
        article_content['title'] = title_elem.text.strip()
    
    # Extract date
    date_elem = soup.find('div', class_='ts-contact-info')
    if date_elem:
        date_text = date_elem.text.strip()
        try:
            article_content['date'] = datetime.strptime(date_text, '%B %d, %Y').date()
        except ValueError:
            article_content['date'] = date_text
    
    # Extract content
    content_div = soup.find('div', class_='ts-article-text')
    if content_div:
        paragraphs = content_div.find_all('p')
        content = []
        for p in paragraphs:
            content.append(p.text.strip())
        article_content['content'] = '\n\n'.join(content)
    
    return article_content
