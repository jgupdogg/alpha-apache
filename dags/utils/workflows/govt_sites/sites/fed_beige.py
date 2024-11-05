from datetime import datetime
import re

def parse_fed_beige_book_links(soup):
    beige_book_entries = []
    current_year = None

    # Find all table rows
    rows = soup.find_all('tr')

    for row in rows:
        # Check if this row contains a year
        year_header = row.find('th', class_='alternate')
        if year_header and year_header.text.strip().isdigit():
            current_year = year_header.text.strip()
            continue

        # Process rows with Beige Book entries
        cells = row.find_all('td')
        if cells and current_year:
            cell_text = cells[0].text.strip()
            date_match = re.match(r'(\w+ \d+):', cell_text)
            
            if date_match:
                date = f"{date_match.group(1)}, {current_year}"
                
                entry = {
                    'date': date,
                    'year': current_year,
                    'link': None
                }

                # Extract HTML link
                links = cells[0].find_all('a')
                for link in links:
                    print(f"Found link text: {link.text.strip()}")  # Debugging line
                    if link.text.strip().lower() == 'html':
                        entry['link'] = f"https://www.federalreserve.gov{link['href']}"
                        break  # Stop after finding the HTML link

                if entry['link']:
                    beige_book_entries.append(entry)
    
    print(beige_book_entries)
    return beige_book_entries


def parse_beige_book_article(soup):
    article_content = {
        'title': '',
        'content': '',
        'date': None
    }
    
    # Extract title
    title_elem = soup.find('h1')
    if title_elem:
        article_content['title'] = title_elem.text.strip()
    
    # Extract date from title or URL
    date_match = re.search(r'beigebook(\d{6})', soup.find('meta', property='og:url')['content'])
    if date_match:
        date_str = date_match.group(1)
        article_content['date'] = datetime.strptime(date_str, '%Y%m').date()
    
    # Find the main article div
    article_div = soup.find('div', id='article')
    if article_div:
        # Extract all text content
        paragraphs = article_div.find_all(['p', 'h3', 'h4', 'h5'])
        content = []
        for p in paragraphs:
            if p.name in ['h3', 'h4', 'h5']:
                content.append(f"\n{p.text.strip()}\n")
            else:
                content.append(p.text.strip())
        article_content['content'] = '\n'.join(content).replace('/n', '  ')
    
    return article_content


