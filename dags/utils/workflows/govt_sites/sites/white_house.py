
from datetime import datetime

def parse_white_house_news_links(soup):
    news_links = []
    
    # Find all news item articles
    news_items = soup.find_all('article', class_='news-item')
    
    for item in news_items:
        news = {}
        
        # Extract title and link
        title_elem = item.find('a', class_='news-item__title')
        if title_elem:
            news['title'] = title_elem.text.strip()
            news['link'] = title_elem['href']
        
        # Extract date
        date_elem = item.find('time', class_='posted-on')
        if date_elem and 'datetime' in date_elem.attrs:
            try:
                news['date'] = datetime.strptime(date_elem['datetime'], '%Y-%m-%dT%H:%M:%S%z').date()
            except ValueError:
                news['date'] = date_elem.text.strip()
        
        # Extract category
        category_elem = item.find('span', class_='cat-links')
        if category_elem:
            category_link = category_elem.find('a')
            if category_link:
                news['category'] = category_link.text.strip()
        
        if news:
            news_links.append(news)
    
    return news_links


def parse_white_house_article_content(soup):
    article_content = {
        'title': '',
        'content': '',
        'date': None
    }
    
    # Extract title
    title_elem = soup.find('h1', class_='page-title')
    if title_elem:
        article_content['title'] = title_elem.text.strip()
    
    # Extract date
    # Note: The date is not visible in the provided HTML snippet.
    # You might need to adjust this part based on where the date is actually located.
    date_elem = soup.find('time', class_='posted-on')
    if date_elem and 'datetime' in date_elem.attrs:
        try:
            article_content['date'] = datetime.strptime(date_elem['datetime'], '%Y-%m-%dT%H:%M:%S%z').date()
        except ValueError:
            article_content['date'] = date_elem.text.strip()
    
    # Extract content
    content_section = soup.find('section', class_='body-content')
    if content_section:
        paragraphs = content_section.find_all('p')
        content = []
        for p in paragraphs:
            # Exclude the centered "###" paragraph which typically marks the end of the content
            if not p.get('class') or 'has-text-align-center' not in p['class']:
                content.append(p.text.strip())
        article_content['content'] = '\n\n'.join(content)
    
    return article_content
