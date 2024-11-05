
from datetime import datetime

def parse_fed_recent_developments(soup):
    recent_developments = []
    
    # Find the "Recent Developments" section
    recent_dev_heading = soup.find('h2', string='Recent Developments')
    if recent_dev_heading:
        # Find the unordered list that follows the heading
        ul_element = recent_dev_heading.find_next('ul', class_='list-unstyled')
        
        if ul_element:
            # Find all list items within the unordered list
            list_items = ul_element.find_all('li')
            
            for item in list_items:
                link = item.find('a')
                if link:
                    title = link.text.strip()
                    href = link.get('href')
                    
                    # Extract the date
                    date_span = item.find('span', class_='time--sm')
                    if date_span:
                        date_text = date_span.text.strip().split(' - ')[-1]
                        try:
                            date = datetime.strptime(date_text, '%m/%d/%Y').date()
                        except ValueError:
                            date = date_text  # If parsing fails, store the original string
                    else:
                        date = None
                    
                    recent_developments.append({
                        'title': title,
                        'link': f"https://www.federalreserve.gov{href}" if href.startswith('/') else href,
                        'date': date
                    })
    
    return recent_developments


def parse_fed_article_content(soup):
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
    content_div = soup.find('div', class_='col-xs-12 col-sm-8 col-md-8')
    if content_div:
        # Extract paragraphs
        paragraphs = content_div.find_all(['p', 'h2', 'h3', 'h4'])
        
        content = []
        for p in paragraphs:
            if p.name in ['h2', 'h3', 'h4']:
                content.append(f"\n{p.text.strip()}\n")
            else:
                text = p.text.strip()
                # Replace footnote superscripts with [n]
                for sup in p.find_all('sup'):
                    text = text.replace(sup.text, f"[{sup.text}]")
                content.append(text)
        
        article_content['content'] = '\n'.join(content)
    
    # Extract date
    date_div = soup.find('div', class_='article__time')
    if date_div:
        date_text = date_div.text.strip()
        try:
            article_content['date'] = datetime.strptime(date_text, '%B %d, %Y').date()
        except ValueError:
            # If date format is different, you might need to adjust the parsing
            pass
    
    return article_content
