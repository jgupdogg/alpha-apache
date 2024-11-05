
from datetime import datetime

def parse_treasury_press_release_links(soup):
    press_releases = []
    
    # Find the main content div
    content_div = soup.find('div', class_='content--2col__body')
    if content_div:
        # Find all press release divs
        release_divs = content_div.find_all('div', recursive=False)
        
        for div in release_divs:
            release = {}
            
            # Extract date
            date_elem = div.find('time', class_='datetime')
            if date_elem and 'datetime' in date_elem.attrs:
                try:
                    release['date'] = datetime.strptime(date_elem['datetime'], '%Y-%m-%dT%H:%M:%SZ').date()
                except ValueError:
                    release['date'] = date_elem.text.strip()
            
            # Extract title and link
            title_elem = div.find('h3', class_='featured-stories__headline')
            if title_elem:
                link_elem = title_elem.find('a')
                if link_elem:
                    release['title'] = link_elem.text.strip()
                    release['link'] = f"https://home.treasury.gov{link_elem['href']}"
            
            # Extract category if available
            category_elem = div.find('span', class_='subcategory')
            if category_elem:
                category_link = category_elem.find('a')
                if category_link:
                    release['category'] = category_link.text.strip()
            
            if release:
                press_releases.append(release)
    
    return press_releases



def parse_treasury_article_content(soup):
    article_content = {
        'title': '',
        'content': '',
        'date': None
    }
    
    # Extract title
    title_elem = soup.find('h2', class_='uswds-page-title')
    if title_elem:
        article_content['title'] = title_elem.text.strip()
    
    # Extract date
    date_elem = soup.find('time', class_='datetime')
    if date_elem and 'datetime' in date_elem.attrs:
        try:
            article_content['date'] = datetime.strptime(date_elem['datetime'], '%Y-%m-%dT%H:%M:%SZ').date()
        except ValueError:
            article_content['date'] = date_elem.text.strip()
    
    # Extract content
    content_div = soup.find('div', class_='field--name-field-news-body')
    if content_div:
        paragraphs = content_div.find_all(['p', 'h2', 'h3', 'h4'])
        content = []
        for p in paragraphs:
            content.append(p.text.strip())
        article_content['content'] = '\n\n'.join(content)
    
    return article_content
