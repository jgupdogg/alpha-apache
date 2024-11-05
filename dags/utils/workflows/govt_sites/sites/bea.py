
from datetime import datetime

# BEA
def parse_bea_blog_articles(soup):
    articles = []
    blog_items = soup.find_all('div', class_='blog-list-item')

    for item in blog_items:
        article = {}

        # Extract title and link
        title_elem = item.find('h3', class_='blog-title')
        if title_elem and title_elem.a:
            article['title'] = title_elem.a.text.strip()
            article['link'] = 'https://www.bea.gov' + title_elem.a['href']

        # Extract date
        date_elem = item.find('div', class_='field-content date-published')
        if date_elem:
            date_str = date_elem.text.strip()
            try:
                article['date'] = datetime.strptime(date_str, '%B %d, %Y').date()
            except ValueError:
                article['date'] = date_str  # If parsing fails, store the original string

        # Extract summary
        summary_elem = item.find('p', class_='blog-summary')
        if summary_elem:
            article['summary'] = summary_elem.text.strip()

        if article:  # Only append if we found some data
            articles.append(article)

    return articles


def parse_bea_article(soup):
    article = {}

    # Extract the date
    date_elem = soup.find('div', class_='date-published')
    if date_elem:
        date_str = date_elem.text.strip()
        try:
            article['date'] = datetime.strptime(date_str, '%B %d, %Y').date()
        except ValueError:
            article['date'] = date_str  # If parsing fails, store the original string

    # Extract the title
    title_elem = soup.find('h1', class_='page-title')
    if title_elem:
        article['title'] = title_elem.text.strip()

    # Extract the main content
    content_elem = soup.find('div', class_='field--name-body')
    if content_elem:
        # Extract all text from paragraphs and list items
        content = []
        for elem in content_elem.find_all(['p', 'li']):
            content.append(elem.get_text(strip=True))
        article['content'] = '\n\n'.join(content)

    return article
