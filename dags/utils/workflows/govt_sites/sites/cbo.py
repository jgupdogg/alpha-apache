
from datetime import datetime
from dags.utils.components.scraper.scraper_api import make_scraper_request
import os
from bs4 import BeautifulSoup

SCRAPER_URL = os.getenv('SCRAPER_URL')



def parse_cbo_article_links(soup):
    article_links = []
    
    # Find the main content div
    content_div = soup.find('div', class_='view-content')
    if content_div:
        # Find all list items
        list_items = content_div.find_all('li')
        
        for item in list_items:
            article = {}
            
            # Extract title and link
            title_span = item.find('span', class_='views-field-title')
            if title_span:
                link_elem = title_span.find('a')
                if link_elem:
                    article['title'] = link_elem.text.strip()
                    article['link'] = f"https://www.cbo.gov{link_elem['href']}"
            
            # Extract date
            date_div = item.find('div', class_='views-field-field-display-date')
            if date_div:
                time_elem = date_div.find('time')
                if time_elem and 'datetime' in time_elem.attrs:
                    date_str = time_elem['datetime']
                    try:
                        article['date'] = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').date()
                    except ValueError:
                        article['date'] = date_str
            
            if article:
                article_links.append(article)
    
    return article_links

def parse_cbo_view_document_link(soup):
    view_document_link = None
    
    # Find the specific div containing the "View Document" link
    view_content = soup.find('div', class_='view-html-report-link')
    if view_content:
        # Find the link with text "View Document"
        link_elem = view_content.find('a', string='View Document')
        if link_elem and 'href' in link_elem.attrs:
            # Extract the href and construct the full URL
            view_document_link = f"https://www.cbo.gov{link_elem['href']}"
    
    return view_document_link

def parse_cbo_article_content(url):
    # Make the initial request to the provided URL
    html_content = make_scraper_request(url)
    
    # Parse the initial soup from the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Get the link to the HTML document from the soup
    html_link = parse_cbo_view_document_link(soup)
    
    # If the view document link is found, request it
    if html_link:
        html_content = make_scraper_request(html_link)
        soup = BeautifulSoup(html_content, 'html.parser')

    article_content = {
        'title': '',
        'content': '',
        'date': None
    }
    
    # Extract title
    title_elem = soup.find('h1')
    if title_elem:
        article_content['title'] = title_elem.text.strip()
    
    # Find the main content div
    content_div = soup.find('div', class_='field--name-body')
    if content_div:
        # Extract all text content
        paragraphs = content_div.find_all(['p', 'h2', 'h3', 'h4'])
        content = []
        for p in paragraphs:
            if p.name in ['h2', 'h3', 'h4']:
                content.append(f"\n{p.text.strip()}\n")
            else:
                content.append(p.text.strip())
        article_content['content'] = '\n'.join(content)
    
    # Extract date (if available)
    date_elem = soup.find('time', class_='datetime')
    if date_elem:
        date_str = date_elem.get('datetime')
        if date_str:
            try:
                article_content['date'] = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').date()
            except ValueError:
                # If date format is different, you might need to adjust the parsing
                pass
    
    return article_content